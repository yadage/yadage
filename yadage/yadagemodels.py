import logging
import adage
import adage.node
import adage.serialize
import adage.nodestate
import jsonpointer
import time
import os
import datetime
import jsonpath_rw
import yadagestep
from backends import NoneProxy
from helpers import get_obj_id, get_id_fromjson
from yadagestep import outputReference

log = logging.getLogger(__name__)


class stage_base(object):
    '''
    Base class for workflow stages (i.e. extension rules)
    provides common datastructures and the extension predicate.
    Implementations are required to provide a schedule()
    method that is called upon apply
    '''

    def __init__(self, name, context, dependencies=None):
        self.view = None
        self.name = name
        self.context = context
        self.depspec = dependencies

    def applicable(self, flowview):
        if not self.depspec:
            return True
        from handlers.predicate_handlers import handlers as pred_handlers
        predicate = pred_handlers[self.depspec['dependency_type']]
        return predicate(flowview, self.depspec)

    def apply(self, flowview):
        self.view = flowview
        self.schedule()

    def addStep(self, step):
        dependencies = [self.view.dag.getNode(k.stepid) for k in step.inputs]

        try:
            depwrites = []
            for d in dependencies:
                try:
                    depwrites += d.task.context['readwrite']
                except AttributeError:
                    pass
            step.context['depwrites'] = depwrites
        except AttributeError:
            pass
        return self.view.addStep(step, stage = self.name, depends_on=dependencies)

    def addWorkflow(self, rules, initstep):
        self.view.addWorkflow(rules, initstep=initstep, stage=self.name)

    #(de-)serialization
    def json(self):
        return {
            'name': self.name,
            'context': self.context,
            'dependencies': self.depspec
        }


class initStage(stage_base):
    '''
    simple stage that just adds a initializer step to the DAG
    '''

    def __init__(self, step, context, dependencies):
        super(initStage, self).__init__('init', context, dependencies)
        self.step = step

    def schedule(self):
        log.debug('initializing a scope with init step: %s',
                  self.step.prepublished)
        self.addStep(self.step)

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        instance = cls(
            step=yadagestep.initstep.fromJSON(data['step']),
            context=data['context'],
            dependencies=data['dependencies']
        )
        return instance

    def json(self):
        data = super(initStage, self).json()
        data.update(type='initStage', info='', step=self.step.json())
        return data


class jsonStage(stage_base):
    '''
    A stage that is defined via the JSON scheduler schemas
    '''

    def __init__(self, json, context):
        self.stageinfo = json['scheduler']
        super(jsonStage, self).__init__(
            json['name'], context, json['dependencies'])

    def __repr__(self):
        return '<jsonStage: {}>'.format(self.name)

    def schedule(self):
        from yadage.handlers.scheduler_handlers import handlers as sched_handlers
        scheduler = sched_handlers[self.stageinfo['scheduler_type']]
        scheduler(self, self.stageinfo)

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        return cls(json={
            'scheduler': data['info'],
            'name': data['name'],
            'dependencies': data['dependencies']
        }, context=data['context'])

    def json(self):
        data = super(jsonStage, self).json()
        data.update(type='jsonStage', info=self.stageinfo)
        return data


class offsetRule(object):
    '''
    A wrapper object around a scoped rule, so that it can be applied from
    a global p.o.v., i.e. as adage expects its rules.
    '''

    def __init__(self, rule, offset=None, identifier=None):
        self.identifier = identifier or get_id_fromjson(
            {'rule': rule.json(), 'offset': offset})
        self.rule = rule
        self.offset = offset

    def __repr__(self):
        return '< offset: "{}" rule: {} >'.format(self.offset,self.rule)

    def applicable(self, adageobj):
        x = self.rule.applicable(WorkflowView(adageobj, self.offset))
        return x

    def apply(self, adageobj):
        self.rule.apply(WorkflowView(adageobj, self.offset))

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        if data['rule']['type'] == 'initStage':
            rule = initStage.fromJSON(data['rule'])
        elif data['rule']['type'] == 'jsonStage':
            rule = jsonStage.fromJSON(data['rule'])
        return cls(
            rule=rule,
            identifier=data['id'],
            offset=data['offset']
        )

    def json(self):
        return {'type': 'offset',
                'id': self.identifier,
                'offset': self.offset,
                'rule': self.rule.json()}


class YadageNode(adage.node.Node):
    '''
    Node object for yadage that extends the default with
    the ability to have prepublished results
    '''

    def __init__(self, name, task, identifier=None):
        super(YadageNode, self).__init__(name, task, identifier)

    def __repr__(self):
        lifetime = datetime.timedelta(seconds = (time.time() - self.define_time))
        runtime = None
        if self.state != adage.nodestate.DEFINED:
            referencetime = time.time() if not self.ready() else self.ready_by_time
            runtime = datetime.timedelta(seconds = (referencetime - self.submit_time))
        return '<YadageNode {} {} lifetime: {}  runtime: {} (id: {}) has result: {}>'.format(
            self.name, self.state, lifetime, runtime, self.identifier, self.has_result()
        )

    def has_result(self):
        if 'YADAGE_IGNORE_PREPUBLISHING' in os.environ: return self.successful()
        return (self.task.prepublished is not None) or self.successful()

    @property
    def result(self):
        if self.task.prepublished is not None and 'YADAGE_IGNORE_PREPUBLISHING' not in os.environ:
            if self.ready() and self.successful():
                sanity =  super(YadageNode, self).result == self.task.prepublished
                if not sanity:
                    raise RuntimeError('prepublished and actual result differ:\nlhs:\n{}\nrhs:{}'.format(
                        super(YadageNode, self).result,self.task.prepublished)
                )
            return self.task.prepublished
        return super(YadageNode, self).result

    def readfromresult(self,pointerpath, whoisreading = None, failsilently = False):
        if not self.has_result():
            if failsilently: return None
            raise RuntimeError('attempt')
        pointer = jsonpointer.JsonPointer(pointerpath)
        if whoisreading:
            whoisreading.inputs.append(outputReference(self.identifier,pointer))
        v = pointer.resolve(self.result)
        return v

    @classmethod
    def fromJSON(cls, data):
        if data['task']['type'] == 'initstep':
            task = yadagestep.initstep.fromJSON(data['task'])
        elif data['task']['type'] == 'yadagestep':
            task = yadagestep.yadagestep.fromJSON(data['task'])
        return cls(data['name'], task, data['id'])


def json_or_nil(x):
    return None if x is None else x.json()


class YadageWorkflow(adage.adageobject):
    '''
    The overall workflow state object that extends the basic
    Adage state object by two bookkeeping structures.
    '''

    def __init__(self):
        super(YadageWorkflow, self).__init__()
        self.stepsbystage = {}
        self.bookkeeping = {}

    def view(self, offset=''):
        return WorkflowView(self, offset)

    def json(self):
        from adage.serialize import obj_to_json
        data = obj_to_json(self,
                           ruleserializer=json_or_nil,
                           taskserializer=json_or_nil,
                           proxyserializer=json_or_nil,
                           )
        data['bookkeeping'] = self.bookkeeping
        data['stepsbystage'] = self.stepsbystage
        return data

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data, proxyclass=NoneProxy, backend=None):
        rules, applied = [], []
        for x in data['rules']:
            rules += [offsetRule.fromJSON(x)]
        for x in data['applied']:
            applied += [offsetRule.fromJSON(x)]

        instance = cls()
        instance.rules = rules
        instance.applied_rules = applied
        instance.bookkeeping = data['bookkeeping']
        instance.stepsbystage = data['stepsbystage']
        instance.dag = adage.serialize.dag_from_json(
            data['dag'],
            YadageNode,
            proxyclass,
            backend
        )
        return instance

    @classmethod
    def createFromJSON(cls, jsondata, context):
        instance = cls()
        # rules = [offsetRule(jsonStage(yml,context), offset = '') for yml in jsondata['stages']]
        rules = [jsonStage(yml, context) for yml in jsondata['stages']]
        rootview = WorkflowView(instance)
        rootview.addWorkflow(rules)
        return instance


def createOffsetMeta(offset, bookkeeping):
    '''
    sets up a location to track rule and step ids for a given scope offset
    '''
    pointer = jsonpointer.JsonPointer(offset)
    view = bookkeeping
    for x in pointer.parts:
        if x not in view:
            view[x] = {}
        view = view[x]
    scoped = pointer.resolve(bookkeeping)
    if '_meta' not in scoped:
        scoped['_meta'] = {'rules': [], 'steps': []}


class WorkflowView(object):
    '''
    Provides a 'view' of the overall workflow object that corresponds
    to a particular level of nesting. That is, this presents the scope
    within which extension rules operate (i.e. add steps, reference
    other steps, etc)
    '''

    def __init__(self, workflowobj, offset=''):
        self.dag = workflowobj.dag
        self.rules = workflowobj.rules
        self.applied_rules = workflowobj.applied_rules
        self.offset = offset
        self.steps = jsonpointer.JsonPointer(
            self.offset).resolve(workflowobj.stepsbystage)
        self.bookkeeper = jsonpointer.JsonPointer(
            self.offset).resolve(workflowobj.bookkeeping)

    def query(self, query, collection):
        matches = jsonpath_rw.parse(query).find(collection)
        return matches

    def getSteps(self, query):
        '''
        returns steps related to the JSONPath query.
        if a query points to a stage (say 'stagename'):
            will return all toplevel steps.. no recursion into subworkflows
        if a query points to steps (e.g. 'stagename[*]')
            will return a steps directly

        '''
        nodeids = []

        matches = self.query(query, self.steps)
        for match in matches:
            value = match.value
            #step endpoint case
            if isinstance(value,dict) and '_nodeid' in value: 
                nodeids.append(value['_nodeid'])
            #stage endpoint case
            elif isinstance(value,list):
                for item in value:
                    if '_nodeid' in item:
                        nodeids.append(item['_nodeid'])

        result = [self.dag.getNode(nodeid) for nodeid in nodeids]
        return result

    def _makeoffset(self, offset):
        thisoffset = jsonpointer.JsonPointer(offset)
        if self.offset:
            fulloffset = jsonpointer.JsonPointer.from_parts(
                jsonpointer.JsonPointer(self.offset).parts + thisoffset.parts).path
        else:
            fulloffset = thisoffset.path
        return fulloffset

    def getRule(self, name=None, offset='', identifier=None):
        '''retrieve a rule by offset or name or identifier'''
        fulloffset = self._makeoffset(offset)
        for x in self.rules + self.applied_rules:
            if x.identifier == identifier or (x.offset == fulloffset and x.rule.name == name):
                return x
        return None

    def init(self, initdata, name='init'):
        step = yadagestep.initstep(name, initdata)
        self.addRule(initStage(step, {}, None), self.offset)

    def addRule(self, rule, offset=''):
        '''
        add a DAG extension rule, possibly with a scope offset
        '''
        thisoffset = jsonpointer.JsonPointer(offset)
        offsetrule = offsetRule(rule, self._makeoffset(offset))
        self.rules += [offsetrule]
        createOffsetMeta(thisoffset.path, self.bookkeeper)
        thisoffset.resolve(self.bookkeeper)['_meta'][
            'rules'] += [offsetrule.identifier]
        return offsetrule.identifier

    def addStep(self, step, stage, depends_on=None):
        '''
        adds a node to the DAG connecting it to the passed depending nodes
        while tracking that it was added by the specified stage
        '''
        node = YadageNode(step.name, step, identifier=get_obj_id(step))
        self.dag.addNode(node, depends_on=depends_on)

        log.debug('added node %s', node)

        noderef = {'_nodeid': node.identifier}
        self.steps.setdefault(stage,[]).append(noderef)
        self.bookkeeper['_meta']['steps'] += [node.identifier]
        return node

    def addWorkflow(self, rules, initstep=None, stage=None):
        if initstep:
            rules += [initStage(initstep, {}, None)]
        newsteps = {}
        if stage in self.steps:
            self.steps[stage] += [newsteps]
        elif stage is not None:
            self.steps[stage] = [newsteps]

        offset = jsonpointer.JsonPointer.from_parts(
            [stage, len(self.steps[stage]) - 1]).path if stage else ''
        if stage is not None:
            self.steps[stage][-1]['_offset'] = offset

        for rule in rules:
            self.addRule(rule, offset)

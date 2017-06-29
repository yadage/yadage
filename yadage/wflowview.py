import logging
import jsonpointer
import jsonpath_rw
from helpers import get_id_fromjson, get_obj_id
from wflownode import YadageNode
from stages import initStage,jsonStage

import yadagestep

log = logging.getLogger(__name__)

class offsetRule(object):
    '''
    A wrapper object around a scoped rule, so that it can be applied from
    a global p.o.v., i.e. as adage expects its rules.
    '''

    def __init__(self, rule, offset=None, identifier=None):
        '''
        initializes a scoped rule. scope is defined by a JSONPointer, e.g.
        one[0]two.three

        '''

        self.rule = rule
        self.offset = offset
        self.identifier = identifier or get_id_fromjson({
            'rule': rule.json(),
            'offset': offset
        })

    def __repr__(self):
        return '<offsetStage {}/{} >'.format(self.offset,self.rule.name)

    def applicable(self, adageobj):
        '''
        determin whether the rule is applicable. Evaluated within the offset.
        :param adageobj: the workflow object
        '''
        x = self.rule.applicable(WorkflowView(adageobj, self.offset))
        return x

    def apply(self, adageobj):
        '''
        applies a rule within the scope set by offset

        :param adageobj: the workflow object
        '''
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
        '''

        :return 
        '''

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
        '''
        prepare a full offset based on this views' offset and a relative offset

        :param offset: the relative offset
        '''
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
        '''
        initialize this scope by adding an initialization stage.

        :param inidata: initialization JSON data
        '''
        step = yadagestep.initstep(name, initdata)
        self.addRule(initStage(step, {}, None), self.offset)

    def addRule(self, rule, offset=''):
        '''
        add a DAG extensloaderion rule, possibly with a scope offset
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
            rules += [initStage(initstep,{},None)]
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

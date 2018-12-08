import logging

import jsonpath_rw
from jsonpointer import JsonPointer

from .stages import JsonStage, OffsetStage
from .utils import get_obj_id, init_stage_spec
from .wflownode import YadageNode

log = logging.getLogger(__name__)



class WorkflowView(object):
    '''
    Provides a 'view' of the overall workflow object that corresponds
    to a particular level of nesting. That is, this presents the scope
    within which extension rules operate (i.e. add steps, reference
    other steps, etc)
    '''

    def __init__(self, workflowobj, offset=''):
        self.wflow = workflowobj
        self.dag = workflowobj.dag
        self.rules = workflowobj.rules
        self.applied_rules = workflowobj.applied_rules
        self.offset = offset
        self.steps = JsonPointer(self.offset).resolve(workflowobj.stepsbystage)
        self.bookkeeper = JsonPointer(self.offset).resolve(workflowobj.bookkeeping)
        self.values = JsonPointer(self.offset).resolve(workflowobj.values)

    def view(self, offset):
        '''
        return a view with an additional offset to this view's offset
        '''
        return self.wflow.view(self._makeoffset(offset))

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
        thisoffset = JsonPointer(offset)
        if self.offset:
            fulloffset = JsonPointer.from_parts(JsonPointer(self.offset).parts + thisoffset.parts).path
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

    def init(self, initdata, init_provider = None, used_inputs = None, name='init', discover = False, relative = True):
        '''
        initialize this scope by adding an initialization stage.

        :param inidata: initialization JSON data
        '''
        spec = init_stage_spec(
            initdata, discover, used_inputs or [], name, relative = relative)
        self.addRule(JsonStage(spec, init_provider), self.offset)

    def addRule(self, rule, offset='', identifier = None):
        '''
        add a DAG extension rule, possibly with a scope offset
        '''
        thisoffset = JsonPointer(offset)
        if offset != '':
            createIndexData(thisoffset.path, self.steps, self.values)
        createOffsetMeta(thisoffset.path, self.bookkeeper)

        offsetstage = OffsetStage(rule, self._makeoffset(offset), identifier = identifier)
        self.rules += [offsetstage]
        thisoffset.resolve(self.bookkeeper)['_meta']['stages'] += [offsetstage.identifier]
        return offsetstage.identifier

    def addValue(self, key, value):
        v = self.values.setdefault('_values', {})
        if key in v:
            raise RuntimeError('cannot overwrite value')
        v[key] = value

    def getValue(self, key):
        return self.values.setdefault('_values', {}).get(key)

    def addStep(self, task, stage, depends_on=None):
        '''
        adds a node to the DAG connecting it to the passed depending nodes
        while tracking that it was added by the specified stage

        :param task: the task object for the step
        :param stage: the stage name
        :param depends_on: dependencies of this step
        '''

        self.steps.setdefault(stage,[])
        node = YadageNode(task.metadata['name'], task, identifier=get_obj_id(task))
        node.task.metadata['wflow_node_id'] = node.identifier
        node.task.metadata['wflow_offset'] = self.offset
        node.task.metadata['wflow_stage'] = stage
        node.task.metadata['wflow_stage_node_idx'] = len(self.steps[stage])
        node.task.metadata['wflow_hints'] = {'is_purepub': task.pubOnlyTask()}

        self.dag.addNode(node, depends_on=depends_on)
        self.steps[stage].append({'_nodeid': node.identifier})
        self.bookkeeper['_meta']['steps'] += [node.identifier]
        log.info('added %s',node)
        return node



    def addWorkflow(self, rules, stage=None):
        '''
        add a (sub-)workflow (i.e. list of stages) to the overall workflow
        '''
        offset = ''
        if stage is not None:
            #make sure storage for the 'authoring' stage is present and
            #register the workflow as part of that 'author'
            #needed e.g. for predicate handlers trying to determing if
            #the author stage is done
            nextindex = len(self.steps.get(stage,[]))
            offset = JsonPointer.from_parts([stage, nextindex]).path

            self.steps.setdefault(stage,[]).append({})
            self.values.setdefault(stage,[]).append({})

        for rule in rules:
            self.addRule(rule, offset)

def createIndexData(offset, stepindex, valueindex):
    pointer = JsonPointer(offset)
    pointer.resolve(stepindex)['_offset'] = offset
    pointer.set(valueindex, {})

def createOffsetMeta(offset, bookkeeping):
    '''
    sets up a location to track rule and step ids for a given scope offset
    '''
    pointer = JsonPointer(offset)
    view = bookkeeping
    for x in pointer.parts:
        view = view.setdefault(x,{})
    pointer.resolve(bookkeeping).setdefault('_meta',{'stages': [], 'steps': []})

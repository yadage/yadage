import logging
from jsonpointer import JsonPointer
import jsonpath_rw
from .utils import get_obj_id, init_stage_spec
from .wflownode import YadageNode
from .stages import JsonStage,OffsetStage

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

    def init(self, initdata, init_provider = None, used_inputs = None, name='init', discover = False):
        '''
        initialize this scope by adding an initialization stage.

        :param inidata: initialization JSON data
        '''
        spec = init_stage_spec(initdata, discover, used_inputs or [], name)
        self.addRule(JsonStage(spec, init_provider), self.offset)

    def addRule(self, rule, offset=''):
        '''
        add a DAG extension rule, possibly with a scope offset
        '''
        thisoffset = JsonPointer(offset)
        offsetstage = OffsetStage(rule, self._makeoffset(offset))
        self.rules += [offsetstage]
        createOffsetMeta(thisoffset.path, self.bookkeeper)
        thisoffset.resolve(self.bookkeeper)['_meta']['stages'] += [offsetstage.identifier]
        return offsetstage.identifier

    def addStep(self, task, stage, depends_on=None):
        '''
        adds a node to the DAG connecting it to the passed depending nodes
        while tracking that it was added by the specified stage

        :param task: the task object for the step
        :param stage: the stage name
        :param depends_on: dependencies of this step
        '''


        node = YadageNode(task.metadata['name'], task, identifier=get_obj_id(task))
        node.task.metadata['wflow_node_id'] = node.identifier
        node.task.metadata['wflow_offset'] = self.offset
        node.task.metadata['wflow_stage'] = stage
        node.task.metadata['wflow_hints'] = {'is_purepub': task.pubOnlyTask()}

        self.dag.addNode(node, depends_on=depends_on)
        self.steps.setdefault(stage,[]).append({'_nodeid': node.identifier})
        self.bookkeeper['_meta']['steps'] += [node.identifier]
        log.info('added node %s', node)
        return node

    def addWorkflow(self, rules, stage=None):
        '''
        add a (sub-)workflow (i.e. list of stages) to the overall workflow
        '''
        offset = ''
        if stage is not None:
            self.steps.setdefault(stage,[]).append({})
            offset = JsonPointer.from_parts([stage, len(self.steps[stage]) - 1]).path
            self.steps[stage][-1]['_offset'] = offset

        for rule in rules:
            self.addRule(rule, offset)

def createOffsetMeta(offset, bookkeeping):
    '''
    sets up a location to track rule and step ids for a given scope offset
    '''
    pointer = JsonPointer(offset)
    view = bookkeeping
    for x in pointer.parts:
        view = view.setdefault(x,{})
    pointer.resolve(bookkeeping).setdefault('_meta',{'stages': [], 'steps': []})

import logging
import adage
import jsonpath_rw
from yadagestep import initstep
log = logging.getLogger(__name__)

STAGESEP = '.'

class stage_base(object):
    def __init__(self,name,context,dependencies):
        self.name = name
        self.context = context
        self.dependencies = dependencies

    def applicable(self,flowview):
        for x in self.dependencies:
            depsteps = flowview.getSteps(x)
            if not depsteps:
                #we expect the dependent stage to have scheduled steps
                return False
            if not all([x.successful() for x in depsteps]):
                return False
        return True

    def apply(self,flowview):
        self.view = flowview
        self.schedule()
    
    def addStep(self,step):
        dependencies = [self.view.dag.getNode(k.stepid) for k in step.inputs]
        self.view.addStep(step, stage = self.name , depends_on = dependencies)

    def addWorkflow(self,rules, initstep, offset):
        self.view.addWorkflow(rules, initstep = initstep, offset = offset)
 
class initStage(stage_base):
    def __init__(self, step, context, dependencies):
        super(initStage,self).__init__('init', context,dependencies)
        self.step = step
    
    def schedule(self):
        self.addStep(self.step)
    
class jsonstage(stage_base):
    def __init__(self,json,context):
        self.stageinfo = json['scheduler']
        super(jsonstage,self).__init__(json['name'],context,json['dependencies'])
        
    def schedule(self):
        from yadage.handlers.scheduler_handlers import handlers as sched_handlers
        scheduler = sched_handlers[self.stageinfo['scheduler_type']]
        scheduler(self,self.stageinfo)
        
class YadageWorkflow(adage.adageobject):
    def __init__(self):
        super(YadageWorkflow,self).__init__()
        self.stepsbystage = {}

    def view(self,offset = None):
        return WorkflowView(self,offset)

    @classmethod
    def fromJSON(cls,jsondata,context):
        instance = cls()
        rules = [jsonstage(yml,context) for yml in jsondata['stages']]
        rootview = WorkflowView(instance)
        rootview.addWorkflow(rules)
        return instance

class offsetRule(object):
    def __init__(self,rule,offset = None):
        self.rule = rule
        self.offset = offset
    
    def applicable(self,adageobj):
        return self.rule.applicable(WorkflowView(adageobj,self.offset))
    
    def apply(self,adageobj):
        self.rule.apply(WorkflowView(adageobj,self.offset))

def offsetdict(base,offset = None):
    if not offset:
        return base
    matches = jsonpath_rw.parse(offset).find(base)
    assert len(matches)==1
    return matches[0].value

class WorkflowView(object):
    def __init__(self,workflowobj,offset = None):
        self.offset = offset
        self.steps  = offsetdict(workflowobj.stepsbystage,offset)
        self.dag    = workflowobj.dag
        self.rules  = workflowobj.rules

    def getSteps(self,query):
        return [self.dag.getNode(step) for match in jsonpath_rw.parse(query).find(self.steps) for step in match.value]
        
    def addStep(self,step, stage, depends_on = None):
        node = self.dag.addTask(step, nodename = step.name, depends_on = depends_on)
        if stage in self.steps:
            self.steps[stage] += [node.identifier]
        else:
            self.steps[stage]  = [node.identifier]
    
    def init(self, initdata, name = 'init'):
        step = initstep(name,initdata)
        self.addRule(initStage(step,{},[]),self.offset)
            
    def addRule(self,rule,offset = None):
        if offset:
            self.steps[offset] = {}
        fulloffset = offset if not self.offset else STAGESEP.join([self.offset,offset])
        self.rules += [offsetRule(rule,fulloffset)]
    
    def addWorkflow(self,rules, initstep = None, offset = None):
        if initstep:
            self.addRule(initStage(initstep,{},[]),offset)
        for rule in rules:
            self.addRule(rule,offset)
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
        self.flowview = flowview
        self.schedule()
    
    def addStep(self,step):
        dependencies = [self.flowview.dag.getNode(k) for k in step.inputs.keys()]
        self.flowview.addStep(step, stage = self.name , depends_on = dependencies)

    def addWorkflow(self,rules, initstep, offset):
        self.flowview.addWorkflow(rules, initstep = initstep, offset = offset)
 
class initStage(stage_base):
    def __init__(self, step, context, dependencies):
        super(initStage,self).__init__('init', context,dependencies)
        self.step = step
    
    def schedule(self):
        self.addStep(self.step)
    
class jsonstage(stage_base):
    def __init__(self,json,context):
        self.stageinfo = json
        super(jsonstage,self).__init__(json['name'],context,json['dependencies'])
        
    def schedule(self):
        from yadage.handlers.scheduler_handlers import handlers as sched_handlers
        scheduler = sched_handlers[self.stageinfo['scheduler']['scheduler_type']]
        scheduler(self,self.stageinfo['scheduler'])
        
class YadageWorkflow(adage.adageobject):
    def __init__(self):
        super(YadageWorkflow,self).__init__()
        self.stepsbystage = {}
    @classmethod
    def fromJSON(cls,jsondata,initdata,context):
        instance = cls()
        rules = [jsonstage(stagedata,context) for stagedata in jsondata]
        rootview = WorkflowView(instance)
        rootview.addWorkflow(rules, initstep = initstep('init root', initdata))
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
    split = offset.rsplit(STAGESEP,1)
    if len(split)==2:
        parent,this = split
        return offsetdict(base,parent)[this]
    else:
        this = split[0]
        return offsetdict(base)[this] 

class WorkflowView(object):
    def __init__(self,workflowobj,offset = None):
        self.offset = offset
        self.steps  = offsetdict(workflowobj.stepsbystage,offset)
        self.dag    = workflowobj.dag
        self.rules  = workflowobj.rules

    def getSteps(self,query):
        return [step for match in jsonpath_rw.parse(query).find(self.steps) for step in match.value]
        
    def addStep(self,step, stage, depends_on = None):
        node = self.dag.addTask(step, nodename = step.name, depends_on = depends_on)
        if stage in self.steps:
            self.steps[stage] += [node]
        else:
            self.steps[stage]  = [node]

    def addRule(self,rule,offset = None):
        if offset:
            self.steps[offset] = {}
        fulloffset = offset if not self.offset else STAGESEP.join([self.offset,offset])
        self.rules += [offsetRule(rule,fulloffset)]
    
    def addWorkflow(self,rules, initstep, offset = None):
        if initstep:
            self.addRule(initStage(initstep,{},[]),offset)
        for rule in rules:
            self.addRule(rule,offset)
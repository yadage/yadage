import logging
import adage
import adage.node
import jsonpointer
import time
import uuid
import jsonpath_rw
from yadagestep import initstep
log = logging.getLogger(__name__)

class wflow_proxy(object):
    def __init__(self,fullrules):
        self.fullrules = fullrules
    
    def done(self,applied_rules):
        return all(rl in applied_rules for rl in self.fullrules)

def walksteps(obj):
    if type(obj) == list:
        for x in obj:
            for o in walksteps(x):
                yield o
    elif type(obj) == dict:
        for k,v in obj.iteritems():
            for o in walksteps(v):
                yield o
    else:
        yield obj

class stage_base(object):
    def __init__(self,name,context,dependencies):
        self.name = name
        self.context = context
        self.dependencies = dependencies

    def applicable(self,flowview):
        for x in self.dependencies:
            depmatches = flowview.query(x,flowview.steps)
            if not depmatches:
                return False
            issubwork = type(depmatches[0].value[0])==dict
            if issubwork:
                depmatches = flowview.query(x,flowview.myrules)
                allsubs =  [proxy for match in depmatches for proxy in match.value]

                allapplied = all([subflow.done(flowview.applied_rules) for subflow in allsubs])
                allstepsok = all([flowview.dag.getNode(x).has_result() for x in walksteps(flowview.steps)])
                if not (allapplied and allstepsok):
                    return False
            else:
                if not all([x.has_result() for x in flowview.getSteps(x)]):
                    return False
        return True

    def apply(self,flowview):
        self.view = flowview
        self.schedule()
    
    def addStep(self,step):
        dependencies = [self.view.dag.getNode(k.stepid) for k in step.inputs]
        self.view.addStep(step, stage = self.name , depends_on = dependencies)

    def addWorkflow(self,rules, initstep, stage):
        self.view.addWorkflow(rules, initstep = initstep, stage = stage)
 
class initStage(stage_base):
    def __init__(self, step, context, dependencies):
        super(initStage,self).__init__('init', context,dependencies)
        self.step = step
    
    def schedule(self):
        self.addStep(self.step)
    
    def json(self):
        return {'type':'initStage','info':'just init', 'name':self.name}
    
class jsonstage(stage_base):
    def __init__(self,json,context):
        self.stageinfo = json['scheduler']
        super(jsonstage,self).__init__(json['name'],context,json['dependencies']['expressions'])
        
    def schedule(self):
        from yadage.handlers.scheduler_handlers import handlers as sched_handlers
        scheduler = sched_handlers[self.stageinfo['scheduler_type']]
        scheduler(self,self.stageinfo)

    def json(self):
        return {'type':'jsonstage','info':self.stageinfo,'name':self.name}

class YadageNode(adage.node.Node):
    def __init__(self,name,task,identifier = None):
        super(YadageNode,self).__init__(name,task,identifier)
    
    def __repr__(self):
        lifetime = time.time()-self.define_time
        return '<YadageNode {} {} lifetime: {} (id: {})>'.format(self.name,self.state,lifetime,self.identifier)

    def has_result(self):
        return (self.task.prepublished is not None) or self.successful()
    
    @property
    def result(self):
        if self.task.prepublished:
            return self.task.prepublished
        return super(YadageNode,self).result

class YadageWorkflow(adage.adageobject):
    def __init__(self):
        super(YadageWorkflow,self).__init__()
        self.stepsbystage = {}
        self.rulesbystage = {} #tracking subworkflow rules

    def view(self,offset = ''):
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
        self.id = uuid.uuid4()
    
    def applicable(self,adageobj):
        return self.rule.applicable(WorkflowView(adageobj,self.offset))
    
    def apply(self,adageobj):
        self.rule.apply(WorkflowView(adageobj,self.offset))

    def json(self):
        return {'type':'offset','offset':self.offset,'rule':self.rule.json(),'id':self.id}

class WorkflowView(object):
    def __init__(self,workflowobj,offset = ''):
        self.offset   = offset
        self.steps    = jsonpointer.JsonPointer(self.offset).resolve(workflowobj.stepsbystage)
        self.myrules  = jsonpointer.JsonPointer(self.offset).resolve(workflowobj.rulesbystage)
        self.dag    = workflowobj.dag
        self.rules  = workflowobj.rules
        self.applied_rules = workflowobj.applied_rules

    def query(self,query,collection):
        matches = jsonpath_rw.parse(query).find(collection)
        return matches

    def getSteps(self,query):
        return [self.dag.getNode(step) for match in self.query(query,self.steps) for step in match.value]
        
    def addStep(self,step, stage, depends_on = None):
        node = YadageNode(step.name,step)
        self.dag.addNode(node,depends_on = depends_on)
        if stage in self.steps:
            self.steps[stage] += [node.identifier]
        else:
            self.steps[stage]  = [node.identifier]
    
    def init(self, initdata, name = 'init'):
        step = initstep(name,initdata)
        self.addRule(initStage(step,{},[]),self.offset)
            
    def addRule(self,rule,offset = ''):
        thisoffset = jsonpointer.JsonPointer(offset)
        if self.offset:
            fulloffset = jsonpointer.JsonPointer.from_parts(jsonpointer.JsonPointer(self.offset).parts + thisoffset.parts).path
        else:
            fulloffset = thisoffset.path

        newrule = offsetRule(rule,fulloffset)
        self.rules += [newrule]
        return newrule
    
    def addWorkflow(self, rules, initstep = None, stage = None):
        newsteps = {}
        if stage in self.steps:
            self.steps[stage] += [newsteps]
        elif stage is not None:
            self.steps[stage]  = [newsteps]
        
        offset = jsonpointer.JsonPointer.from_parts([stage,len(self.steps[stage])-1]).path if stage else ''
        
        if initstep:
            self.addRule(initStage(initstep,{},[]),offset)

        fullrules = [self.addRule(rule,offset) for rule in rules]
        if stage in self.myrules:
            self.myrules[stage] += [wflow_proxy(fullrules)]
        elif stage is not None:
            self.myrules[stage]  = [wflow_proxy(fullrules)]
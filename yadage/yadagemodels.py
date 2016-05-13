import logging
import adage
import adage.node
import jsonpointer
import time
import uuid
import jsonpath_rw
from yadagestep import initstep
log = logging.getLogger(__name__)

class stage_base(object):
    def __init__(self,name,context,dependencies = None):
        self.name = name
        self.context = context
        self.depspec = dependencies
        
    def applicable(self,flowview):
        if not self.depspec: return True
        from handlers.predicate_handlers import handlers as pred_handlers
        predicate = pred_handlers[self.depspec['dependency_type']]
        return predicate(flowview,self.depspec)
        
    def apply(self,flowview):
        log.debug('applying stage: %s',self.name)
        self.view = flowview
        self.schedule()
        
    def addStep(self,step):
        dependencies = [self.view.dag.getNode(k.stepid) for k in step.inputs]
        self.view.addStep(step, stage = self.name, depends_on = dependencies)

    def addWorkflow(self,rules, initstep):
        self.view.addWorkflow(rules, initstep = initstep, stage = self.name)
 
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
        super(jsonstage,self).__init__(json['name'],context,json['dependencies'])
        
    def schedule(self):
        from yadage.handlers.scheduler_handlers import handlers as sched_handlers
        scheduler = sched_handlers[self.stageinfo['scheduler_type']]
        scheduler(self,self.stageinfo)

    def json(self):
        return {'type':'jsonstage','info':self.stageinfo,'name':self.name}

class offsetRule(object):
    def __init__(self,rule,offset = None):
        self.identifier = str(uuid.uuid4())
        self.rule = rule
        self.offset = offset
    
    def applicable(self,adageobj):
        return self.rule.applicable(WorkflowView(adageobj,self.offset))
    
    def apply(self,adageobj):
        self.rule.apply(WorkflowView(adageobj,self.offset))
        
    def json(self):
        return {'type':'offset','offset':self.offset,'rule':self.rule.json(),'id':self.identifier}

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
        self.bookkeeping = {'_meta':{'rules':[],'steps':[]}}
        
    def view(self,offset = ''):
        return WorkflowView(self,offset)
        
    def json(self):
        from adage.serialize import obj_to_json
        data = obj_to_json(self,ruleserializer = lambda r:r.json(), taskserializer = lambda t:t.json(), proxyserializer = lambda p: p.json())
        data['bookkeeping']=self.bookkeeping
        return data
        
    @classmethod
    def fromJSON(cls,jsondata,context):
        instance = cls()
        rules = [jsonstage(yml,context) for yml in jsondata['stages']]
        rootview = WorkflowView(instance)
        rootview.addWorkflow(rules)
        return instance

class WorkflowView(object):
    def __init__(self,workflowobj,offset = ''):
        self.dag           = workflowobj.dag
        self.rules         = workflowobj.rules
        self.applied_rules = workflowobj.applied_rules
        
        self.offset        = offset
        self.steps         = jsonpointer.JsonPointer(self.offset).resolve(workflowobj.stepsbystage)
        self.bookkeeper    = jsonpointer.JsonPointer(self.offset).resolve(workflowobj.bookkeeping)
        
        
    def query(self,query,collection):
        matches = jsonpath_rw.parse(query).find(collection)
        return matches
        
    def getSteps(self,query):
        result =  [self.dag.getNode(step['_nodeid']) for match in self.query(query,self.steps) for step in match.value]
        return result
        
    def init(self, initdata, name = 'init'):
        step = initstep(name,initdata)
        self.addRule(initStage(step,{},None),self.offset)
            
    def addRule(self,rule,offset = ''):
        
        thisoffset = jsonpointer.JsonPointer(offset)
        if self.offset:
            fulloffset = jsonpointer.JsonPointer.from_parts(jsonpointer.JsonPointer(self.offset).parts + thisoffset.parts).path
        else:
            fulloffset = thisoffset.path
        offsetrule = offsetRule(rule,fulloffset)
        self.rules += [offsetrule]
        thisoffset.resolve(self.bookkeeper)['_meta']['rules'] += [offsetrule.identifier]
        return offsetrule.identifier
    
    def addStep(self,step, stage, depends_on = None):
        node = YadageNode(step.name,step)
        self.dag.addNode(node,depends_on = depends_on)
        
        noderef = {'_nodeid':node.identifier}
        if stage in self.steps:
            self.steps[stage] += [noderef]
        else:
            self.steps[stage]  = [noderef]
        self.bookkeeper['_meta']['steps'] += [node.identifier]

    def addWorkflow(self, rules, initstep = None, stage = None):
        if initstep:
            rules += [initStage(initstep,{},None)]
        
        newsteps = {}
        if stage in self.steps:
            self.steps[stage] += [newsteps]
        elif stage is not None:
            self.steps[stage]  = [newsteps]

        offset = jsonpointer.JsonPointer.from_parts([stage,len(self.steps[stage])-1]).path if stage else ''
        if stage is not None:
            self.steps[stage][-1]['_offset'] = offset
        
        booker = self.bookkeeper
        for p in jsonpointer.JsonPointer(offset).parts:
            if p in booker:
                pass
            else:
                booker[p] = {'_meta':{'rules':[],'steps':[]}}
            booker = booker[p]
        
        for rule in rules:
            self.addRule(rule,offset)
        
        
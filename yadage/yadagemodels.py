import logging
log = logging.getLogger(__name__)

class workflow(object):
    def __init__(self,context):
        self.context = context
        self.stages = {}
    
    def stage(self,name):
        return self.stages[name]
    
    @classmethod
    def fromJSON(cls,json,context):
        instance = cls(context)
        stages = {}
        for stagejson in json['stages']:
            stages[stagejson['name']] = stage(stagejson,instance,context)
        instance.stages = stages
        return instance

class stage_base(object):
    def __init__(self,workflow,context,dependencies):
        self.context = context
        self.workflow = workflow
        self.dependencies = dependencies
        self.scheduled_steps = []
        
    def applicable(self,dag):
        for x in self.dependencies:
            deprule = self.workflow.stage(x)
            if not deprule.scheduled_steps:
                return False
            elif not all([x.successful() for x in deprule.scheduled_steps]):
                return False
        return True
    
    def addStep(self,task):
        dependencies = [self.dag.getNode(k) for k in task.inputs.keys()]
        node = self.dag.addTask(task, nodename = task.name, depends_on = dependencies)
        self.scheduled_steps += [node]
    
    def apply(self,dag):
        self.dag = dag
        self.schedule()
    
class stage(stage_base):
    def __init__(self,stageinfo,workflow,context):
        self.stageinfo = stageinfo
        super(stage,self).__init__(workflow,context,stageinfo['dependencies'])

    def schedule(self):
        from yadage.handlers.scheduler_handlers import handlers as sched_handlers
        sched_spec = self.stageinfo['scheduler']
        scheduler = sched_handlers[sched_spec['scheduler_type']]
        scheduler(self,sched_spec)    
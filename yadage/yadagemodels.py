import logging
log = logging.getLogger(__name__)

class workflow(object):
    def __init__(self,context):
        self.context = context
        self.stages = {}
    
    def stage(self,name):
        return self.stages[name]
    
    def addStage(self,stage):
        if stage.name in self.stages:
            raise RuntimeError('duplicate stage with name %s', stage.name)
        self.stages[stage.name] = stage
    
    @classmethod
    def fromJSON(cls,json,context):
        instance = cls(context)
        for stagejson in json['stages']:
            stage = jsonstage(stagejson,instance,context)
            instance.addStage(stage)
        return instance

class stage_base(object):
    def __init__(self,name,workflow,context,dependencies):
        self.name = name
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
    
class jsonstage(stage_base):
    def __init__(self,json,workflow,context):
        self.stageinfo = json
        super(jsonstage,self).__init__(json['name'],workflow,context,json['dependencies'])

    def schedule(self):
        from yadage.handlers.scheduler_handlers import handlers as sched_handlers
        sched_spec = self.stageinfo['scheduler']
        scheduler = sched_handlers[sched_spec['scheduler_type']]
        scheduler(self,sched_spec)
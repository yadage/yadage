import adage.backends
import logging
import yadagestep
log = logging.getLogger(__name__)

class yadage_task(object):
    def __init__(self,step,context):
        self.func = yadagestep.runstep
        self.step = step
        self.context = context

    def __repr__(self):
        return '<yadage task name: {}>'.format(self.step['name'])

    def __call__(self):
        return self.func(self.step,self.context)

class yadage_rule(object):
    def __init__(self,stageinfo,workflow,allrules,global_context):
        self.stageinfo = stageinfo
        self.global_context = global_context
        self.workflow = workflow
        self.allrules = allrules
  
    def applicable(self,dag):
        depstats = []
        for x in self.stageinfo['dependencies']:
            deprule = self.allrules[x]
            if not 'scheduled_steps' in deprule.stageinfo:
                depstats += [False]
            else:
                depstats += [all([x.successful() for x in deprule.stageinfo['scheduled_steps']])]
                
        return all(depstats)

    def apply(self,dag):
        self.schedule(dag)
    
    def schedule(self,dag):
        from yadage.handlers.scheduler_handlers import handlers as sched_handlers
        sched_spec = self.stageinfo['scheduler']
        scheduler = sched_handlers[sched_spec['scheduler-type']]
        scheduler(self.workflow,self.stageinfo,dag,self.global_context,sched_spec)
    
class yadage_result(object):
    def __init__(self,resultobj,task):
        self.resultobj = resultobj
        self.task = task
        self.result = None
    
    def ready(self):
        return self.resultobj.ready()
    def successful(self):
        return self.resultobj.successful()
    def get(self):
        if self.result:
            return self.result
        try:
            taskresult = self.resultobj.get()
        except:
            log.exception("taskresult retrieval failed")
            raise
        
        #cache the result
        self.result = taskresult
        return self.result

class yadage_backend(adage.backends.MultiProcBackend):
    def submit(self,task):
        return yadage_result(super(yadage_backend,self).submit(task),task)

    def ready(self,result):
        ready =  super(yadage_backend,self).ready(result)
        return ready
    
    def result_of(self,result):
        return result.get()

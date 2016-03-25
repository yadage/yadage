import logging
import yadagestep

log = logging.getLogger(__name__)

class init_rule(object):
    def __init__(self,stageinfo,initdata):
        self.initdata = initdata
        self.stageinfo = stageinfo
        
    def apply(self,dag):
        node = dag.addTask(yadagestep.initstep(self.initdata),nodename = 'init')
        self.stageinfo['scheduled_steps']=[node]
        
    def applicable(self,dag):
        return True

class yadage_rule(object):
    def __init__(self,stageinfo,workflow,allrules,context):
        self.stageinfo = stageinfo
        self.context = context
        self.workflow = workflow
        self.allrules = allrules
  
    def applicable(self,dag):
        depstats = []
        for x in self.stageinfo['dependencies']:
            deprule = self.allrules[x]
            if 'scheduled_steps' not in deprule.stageinfo:
                depstats += [False]
            else:
                depstats += [all([x.successful() for x in deprule.stageinfo['scheduled_steps']])]
        return all(depstats)

    def apply(self,dag):
        self.schedule(dag)
    
    def schedule(self,dag):
        from yadage.handlers.scheduler_handlers import handlers as sched_handlers
        sched_spec = self.stageinfo['scheduler']
        scheduler = sched_handlers[sched_spec['scheduler_type']]
        scheduler(self.workflow,self.stageinfo,dag,self.context,sched_spec)
    

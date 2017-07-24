import tasks
import logging
from handlers.predicate_handlers import handlers as pred_handlers
from packtivity.statecontexts import load_provider

log = logging.getLogger(__name__)

class ViewStageBase(object):
    '''
    base class for workflow stages operating on workflow views, that may
    expose only a partial slice of an overall workflow.
    The class also provides common methods to ease modifying the workflow.
    
    Implementations are required to provide
    * a ready() method to implement the predicate method
    * a schedule() method that is called upon apply
    '''
    def __init__(self, name, state_provider):
        self.name = name
        self.state_provider = state_provider
        self.view = None

    def schedule(self):
        raise NotImplementedError()

    def ready(self):
        raise NotImplementedError()

    def applicable(self, flowview):
        self.view = flowview
        return self.ready()

    def apply(self, flowview):
        self.view = flowview
        self.schedule()

    def addStep(self, step):
        dependencies = [self.view.dag.getNode(k.stepid) for k in step.inputs]
        for d in dependencies:
            try:
                step.state.add_dependency(d.task.state)
            except AttributeError:
                pass
        return self.view.addStep(step, stage = self.name, depends_on=dependencies)

    def addWorkflow(self, rules, initstep):
        self.view.addWorkflow(rules, initstep=initstep, stage=self.name)

    #(de-)serialization
    def json(self):
        return {
            'name': self.name,
            'state_provider': self.state_provider.json() if self.state_provider else None
        }

class InitStage(ViewStageBase):
    '''
    simple stage that just adds a initializer step to the DAG
    '''

    def __init__(self, step):
        super(InitStage, self).__init__('init', None)
        self.step = step

    def applicable(self, flowview):
        return True

    def schedule(self):
        log.debug('initializing a scope with init step: %s',
                  self.step.prepublished)
        self.addStep(self.step)

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        instance = cls(
            step = tasks.init_task.fromJSON(data['step'])
        )
        return instance

    def json(self):
        data = super(InitStage, self).json()
        data.update(type='InitStage', info='', step=self.step.json())
        return data


class JsonStage(ViewStageBase):
    '''
    A stage that is defined via the JSON scheduler schemas
    '''

    def __init__(self, json, state_provider):
        self.stageinfo = json['scheduler']
        self.depspec = json['dependencies']
        super(JsonStage, self).__init__(json['name'], state_provider)

    def __repr__(self):
        return '<JsonStage: {}>'.format(self.name)

    def ready(self):
        if not self.depspec:
            return True
        predicate = pred_handlers[self.depspec['dependency_type']]
        return predicate(self, self.depspec)

    def schedule(self):
        #imported here to avoid circular dependency
        from handlers.scheduler_handlers import handlers as sched_handlers
        scheduler = sched_handlers[self.stageinfo['scheduler_type']]
        scheduler(self, self.stageinfo)

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        return cls(
            json={
                'scheduler': data['info'],
                'name': data['name'],
                'dependencies': data['dependencies']
            },
            state_provider=load_provider(data['state_provider'])
        )

    def json(self):
        data = super(JsonStage, self).json()
        data.update(type='JsonStage', info=self.stageinfo, dependencies = self.depspec)
        return data

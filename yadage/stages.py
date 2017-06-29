import yadagestep
import logging
from handlers.predicate_handlers import handlers as pred_handlers
from packtivity.statecontexts import load_provider

log = logging.getLogger(__name__)

class StageBase(object):
    '''
    Base class for workflow stages (i.e. extension rules)
    provides common datastructures and the extension predicate.
    Implementations are required to provide a schedule()
    method that is called upon apply
    '''

    def __init__(self, name, state_provider, dependencies=None):
        self.view = None
        self.name = name
        self.state_provider = state_provider
        self.depspec = dependencies

    def applicable(self, flowview):
        if not self.depspec:
            return True
        predicate = pred_handlers[self.depspec['dependency_type']]
        return predicate(flowview, self.depspec)

    def apply(self, flowview):
        self.view = flowview
        self.schedule()

    def schedule(self):
        raise NotImplementedError()

    def addStep(self, step):
        dependencies = [self.view.dag.getNode(k.stepid) for k in step.inputs]
        for d in dependencies:
            try:
                step.context.add_dependency(d.task.context)
            except AttributeError:
                pass
        return self.view.addStep(step, stage = self.name, depends_on=dependencies)

    def addWorkflow(self, rules, initstep):
        self.view.addWorkflow(rules, initstep=initstep, stage=self.name)

    #(de-)serialization
    def json(self):
        return {
            'name': self.name,
            'state_provider': self.state_provider.json() if self.state_provider else None,
            'dependencies': self.depspec
        }


class initStage(StageBase):
    '''
    simple stage that just adds a initializer step to the DAG
    '''

    def __init__(self, step, state_provider, dependencies):
        super(initStage, self).__init__('init', state_provider, dependencies)
        self.step = step

    def schedule(self):
        log.debug('initializing a scope with init step: %s',
                  self.step.prepublished)
        self.addStep(self.step)

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        instance = cls(
            step = yadagestep.initstep.fromJSON(data['step']),
            state_provider = load_provider(data['state_provider']),
            dependencies = data['dependencies']
        )
        return instance

    def json(self):
        data = super(initStage, self).json()
        data.update(type='initStage', info='', step=self.step.json())
        return data


class jsonStage(StageBase):
    '''
    A stage that is defined via the JSON scheduler schemas
    '''

    def __init__(self, json, state_provider):
        self.stageinfo = json['scheduler']
        super(jsonStage, self).__init__(
            json['name'], state_provider, json['dependencies'])

    def __repr__(self):
        return '<jsonStage: {}>'.format(self.name)

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
        data = super(jsonStage, self).json()
        data.update(type='jsonStage', info=self.stageinfo)
        return data

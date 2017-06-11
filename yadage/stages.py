import logging
from handlers.predicate_handlers import handlers as pred_handlers
import yadagestep

log = logging.getLogger(__name__)

class StageBase(object):
    '''
    Base class for workflow stages (i.e. extension rules)
    provides common datastructures and the extension predicate.
    Implementations are required to provide a schedule()
    method that is called upon apply
    '''

    def __init__(self, name, context, dependencies=None):
        self.view = None
        self.name = name
        self.context = context
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

        try:
            depwrites = []
            for d in dependencies:
                try:
                    depwrites += d.task.context['readwrite']
                except AttributeError:
                    pass
            step.context['depwrites'] = depwrites
        except AttributeError:
            pass
        return self.view.addStep(step, stage = self.name, depends_on=dependencies)

    def addWorkflow(self, rules, initstep):
        self.view.addWorkflow(rules, initstep=initstep, stage=self.name)

    #(de-)serialization
    def json(self):
        return {
            'name': self.name,
            'context': self.context,
            'dependencies': self.depspec
        }


class initStage(StageBase):
    '''
    simple stage that just adds a initializer step to the DAG
    '''

    def __init__(self, step, context, dependencies):
        super(initStage, self).__init__('init', context, dependencies)
        self.step = step

    def schedule(self):
        log.debug('initializing a scope with init step: %s',
                  self.step.prepublished)
        self.addStep(self.step)

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        instance = cls(
            step=yadagestep.initstep.fromJSON(data['step']),
            context=data['context'],
            dependencies=data['dependencies']
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

    def __init__(self, json, context):
        self.stageinfo = json['scheduler']
        super(jsonStage, self).__init__(
            json['name'], context, json['dependencies'])

    def __repr__(self):
        return '<jsonStage: {}>'.format(self.name)

    def schedule(self):
        from handlers.scheduler_handlers import handlers as sched_handlers
        scheduler = sched_handlers[self.stageinfo['scheduler_type']]
        scheduler(self, self.stageinfo)

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        return cls(json={
            'scheduler': data['info'],
            'name': data['name'],
            'dependencies': data['dependencies']
        }, context=data['context'])

    def json(self):
        data = super(jsonStage, self).json()
        data.update(type='jsonStage', info=self.stageinfo)
        return data

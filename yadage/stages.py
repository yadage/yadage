import logging

from .handlers.predicate_handlers import handlers as pred_handlers
from .utils import get_id_fromjson
from .state_providers import load_provider

from packtivity import datamodel as _datamodel

log = logging.getLogger(__name__)


class OffsetStage(object):
    '''
    A wrapper object around a scoped rule, so that it can be applied from
    a global p.o.v., i.e. as adage expects its rules.
    '''

    def __init__(self, rule, offset=None, identifier=None):
        '''
        initializes a scoped rule. scope is defined by a JSONPointer, e.g.
        one[0]two.three

        '''

        self.rule = rule
        self.offset = offset
        self.identifier = identifier or get_id_fromjson({
            'rule': rule.json(),
            'offset': offset
        })

    def __repr__(self):
        return '<OffsetStage {}/{} >'.format(self.offset,self.rule.name)

    def applicable(self, adageobj):
        '''
        determin whether the rule is applicable. Evaluated within the offset.
        :param adageobj: the workflow object
        '''
        from .wflowview import WorkflowView #importing here to avoid circdep
        x = self.rule.applicable(WorkflowView(adageobj, self.offset))
        return x

    def apply(self, adageobj):
        '''
        applies a rule within the scope set by offset

        :param adageobj: the workflow object
        '''
        from .wflowview import WorkflowView #importing here to avoid circdep
        self.rule.apply(WorkflowView(adageobj, self.offset))

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data, deserialization_opts = None):
        if data['rule']['type'] == 'JsonStage':
            rule = JsonStage.fromJSON(data['rule'], deserialization_opts)
        else:
            RuntimeError('unknown stage type %s', data['rule']['type'])
        return cls(
            rule=rule,
            identifier=data['id'],
            offset=data['offset']
        )

    def json(self):
        return {'type': 'offset',
                'id': self.identifier,
                'offset': self.offset,
                'rule': self.rule.json()}


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
        self.datamodel = _datamodel
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
        return self.view.addStep(step, stage = self.name, depends_on=dependencies)

    def addWorkflow(self, rules, isolate = True):
        self.view.addWorkflow(rules, stage=self.name if isolate else None)

    def json(self):
        return {
            'name': self.name,
            'state_provider': self.state_provider.json() if self.state_provider else None
        }

class JsonStage(ViewStageBase):
    '''
    A stage that is defined via the JSON scheduler schemas
    '''

    def __init__(self, json, state_provider):
        self.stagespec = json['scheduler']
        self.depspec = json['dependencies']
        super(JsonStage, self).__init__(json['name'], state_provider)

    def __repr__(self):
        return '<JsonStage: {}>'.format(self.name)

    def ready(self):
        if not self.depspec:
            return True
        predicate = pred_handlers[self.depspec['dependency_type']]
        return predicate(self, self.depspec, self.stagespec)

    def schedule(self):
        #imported here to avoid circular dependency
        from .handlers.scheduler_handlers import handlers as sched_handlers
        scheduler = sched_handlers[self.stagespec['scheduler_type']]
        scheduler(self, self.stagespec)

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data, deserialization_opts = None):
        return cls(
            json={
                'scheduler': data['scheduler'],
                'name': data['name'],
                'dependencies': data['dependencies']
            },
            state_provider=load_provider(data['state_provider'],deserialization_opts)
        )

    def json(self):
        data = super(JsonStage, self).json()
        data.update(type='JsonStage', scheduler=self.stagespec, dependencies = self.depspec)
        return data

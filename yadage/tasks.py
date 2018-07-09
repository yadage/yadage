import logging

from packtivity.typedleafs import TypedLeafs

from .utils import outputReference

log = logging.getLogger(__name__)

class packtivity_task(object):
    '''
    packtivity task
    '''
    def __init__(self, name, spec, state, parameters = None, inputs = None):
        self.metadata = {'name': name}
        self.inputs = inputs or []
        self.parameters = TypedLeafs(parameters or {}, state.datamodel if state else None)
        self.spec = spec
        self.state = state

    def pubOnlyTask(self):
        return (self.spec['environment'] is None) and (self.spec['process'] is None)

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data, state_deserializer):
        instance = cls(
            data['metadata']['name'],
            data['spec'],
            state_deserializer(data['state']) if data['state'] else None,
            data['parameters'],
            inputs = map(outputReference.fromJSON, data['inputs'])
        )
        instance.metadata.update(**data['metadata'])
        return instance

    def json(self):
        return {
            'metadata': self.metadata,
            'parameters': self.parameters.json(),
            'inputs': [x.json() for x in self.inputs],
            'type': 'packtivity_task',
            'spec': self.spec,
            'state': self.state.json() if self.state else None,
        }

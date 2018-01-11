import packtivity
import logging
from .utils import outputReference
from simplytyped.typed import TypedLeafs
log = logging.getLogger(__name__)

class packtivity_task(object):
    '''
    packtivity task
    '''
    def __init__(self, name, spec, state):
        self.metadata = {'name': name}
        self.inputs = []
        self.parameters = TypedLeafs({})
        self.prepublished = None
        self.spec = spec
        self.state = state

    def used_input(self, reference):
        self.inputs += [reference]

    def used_inputs(self, references):
        for r in references: self.used_input(r)

    def pubOnlyTask(self):
        return (self.spec['environment'] is None) and (self.spec['process'] is None)

    def s(self, **parameters):
        self.parameters.update(**parameters)

        # attempt to prepublish output data merely from inputs
        # will still be None if not possible
        self.prepublished = TypedLeafs(packtivity.prepublish_default(self.spec, self.parameters.json(), self.state))
        log.debug('parameters for packtivity_task set to %s. prepublished result, if any: %s',
                    self.parameters,
                    self.prepublished
        )
        return self

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data, state_deserializer):
        instance = cls(data['metadata']['name'], data['spec'], state_deserializer(data['state']) if data['state'] else None)
        instance.parameters = TypedLeafs(data['parameters'])
        instance.prepublished = TypedLeafs(data['prepublished']) if data['prepublished'] else None
        instance.inputs = map(outputReference.fromJSON, data['inputs'])
        instance.metadata.update(**data['metadata'])
        return instance

    def json(self):
        data = {
            'metadata': self.metadata,
            'parameters': self.parameters.json(),
            'prepublished': self.prepublished.json() if self.prepublished else None,
            'inputs': [x.json() for x in self.inputs]
        }
        data.update(
            type='packtivity_task',
            spec=self.spec,
            state=self.state.json() if self.state else None,
        )
        return data

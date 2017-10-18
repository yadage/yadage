import packtivity
import logging
from .utils import leaf_iterator_jsonlike, outputReference

log = logging.getLogger(__name__)



class TaskBase(object):
    def __init__(self, **metadata):
        self.metadata = metadata
        self.inputs = []
        self.parameters = {}
        self.prepublished = None

    def used_input(self, reference):
        self.inputs += [reference]

    def used_inputs(self, references):
        for r in references: self.used_input(r)

    #(de-)serialization
    def json(self):
        return {
            'metadata': self.metadata,
            'parameters': self.parameters,
            'prepublished': self.prepublished,
            'inputs': [x.json() for x in self.inputs]
        }

class init_task(TaskBase):
    '''
    initialization task
    '''

    def __init__(self, name, initdata=None):
        super(init_task, self).__init__(name = name)
        self.prepublished = None
        if initdata is not None:
            self.s(**initdata)

    def s(self, **parameters):
        self.parameters = parameters
        self.prepublished = self.parameters
        return self

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        instance = cls(data['metadata']['name'])
        instance.parameters = data['parameters']
        instance.prepublished = data['prepublished']
        instance.inputs = map(outputReference.fromJSON, data['inputs'])
        return instance

    def json(self):
        data = super(init_task, self).json()
        data.update(type='init_task')
        return data


class packtivity_task(TaskBase):
    '''
    packtivity task
    '''
    def __init__(self, name, spec, state):
        super(packtivity_task, self).__init__(name = name)
        self.spec = spec
        self.state = state

    def s(self, **parameters):
        self.parameters.update(**parameters)
        for leaf_pointer, leaf_value in leaf_iterator_jsonlike(self.parameters):
            leaf_pointer.set(self.parameters,self.state.contextualize_data(leaf_value))
        # attempt to prepublish output data merely from inputs
        # will still be None if not possible
        self.prepublished = packtivity.prepublish_default(self.spec, self.parameters, self.state)
        log.debug('parameters for packtivity_task set to %s. prepublished result, if any: %s',
                    self.parameters,
                    self.prepublished
                  )
        return self

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data, state_deserializer):
        instance = cls(data['metadata']['name'], data['spec'], state_deserializer(data['state']))
        instance.parameters = data['parameters']
        instance.prepublished = data['prepublished']
        instance.inputs = map(outputReference.fromJSON, data['inputs'])
        return instance

    def json(self):
        data = super(packtivity_task, self).json()
        data.update(
            type='packtivity_task',
            spec=self.spec,
            state=self.state.json(),
        )
        return data

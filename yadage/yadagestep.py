import packtivity
import logging
import jsonpointer
from packtivity.statecontexts import load_state

log = logging.getLogger(__name__)

class outputReference(object):

    def __init__(self, stepid, pointer):
        self.stepid = stepid
        self.pointer = pointer

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        return cls(data['stepid'], jsonpointer.JsonPointer(data['pointer_path']))

    def json(self):
        return {
            'stepid': self.stepid,
            'pointer_path': self.pointer.path
        }

class StepBase(object):

    def __init__(self, name):
        self.name = name
        self.inputs = []
        self.attributes = {}
        self.prepublished = None

    def used_input(self, reference):
        self.inputs += [reference]

    #(de-)serialization
    def json(self):
        return {
            'name': self.name,
            'attributes': self.attributes,
            'prepublished': self.prepublished,
            'inputs': [x.json() for x in self.inputs]
        }

class initstep(StepBase):

    def __init__(self, name, initdata=None):
        super(initstep, self).__init__(name)
        self.prepublished = None
        if initdata is not None:
            self.s(**initdata)

    def s(self, **attributes):
        self.attributes = attributes
        self.prepublished = self.attributes
        return self

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        instance = cls(data['name'])
        instance.attributes = data['attributes']
        instance.prepublished = data['prepublished']
        instance.inputs = map(outputReference.fromJSON, data['inputs'])
        return instance

    def json(self):
        data = super(initstep, self).json()
        data.update(type='initstep')
        return data


class yadagestep(StepBase):

    def __init__(self, name, spec, context):
        super(yadagestep, self).__init__(name)
        self.spec = spec
        self.context = context

    def s(self, **attributes):
        self.attributes.update(**attributes)
        # attempt to prepublish output data merely from inputs
        # will still be None if not possible
        self.prepublished = packtivity.prepublish_default(self.spec, self.attributes, self.context)
        log.debug('parameters for yadagestep set to %s. prepublished result, if any: %s',
                    self.attributes,
                    self.prepublished
                  )
        return self

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        instance = cls(data['name'], data['spec'], load_state(data['context']))
        instance.attributes = data['attributes']
        instance.prepublished = data['prepublished']
        instance.inputs = map(outputReference.fromJSON, data['inputs'])
        return instance

    def json(self):
        data = super(yadagestep, self).json()
        data.update(
            type='yadagestep',
            spec=self.spec,
            context=self.context.json(),
        )
        return data

import packtivity
import logging
log = logging.getLogger(__name__)

class outputReference(object):
    def __init__(self,stepid,pointer):
        self.stepid = stepid
        self.pointer = pointer

class stepbase(object):
    def __init__(self,name):
        self.name = name
        self.inputs = []
        self.attributes = {}
        
    def used_input(self,reference):
        self.inputs += [reference]
    
class initstep(stepbase):
    def __init__(self,name, initdata = None):
        super(initstep,self).__init__(name)
        self.prepublished = None
        if initdata:
            self.attributes = initdata
    
    def __call__(self):
        self._result = self.attributes
        return self._result

    def s(self,**attributes):
        self.attributes = attributes
        self.prepublished = self.attributes
        return self
        
class yadagestep(stepbase):
    def __init__(self,name,spec,context):
        super(yadagestep,self).__init__(name)
        self.spec = spec
        self.context = context
        self.p = None
        
    def s(self,**attributes):
        self.attributes.update(**attributes)
        self.prepublished = packtivity.prepublish(self.spec,self.attributes,self.context)
        return self
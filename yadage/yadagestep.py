from packtivity import packtivity

class outputReference(object):
    def __init__(self,stepid,pointer):
        self.stepid = stepid
        self.pointer = pointer

class stepbase(object):
    def __init__(self,name):
        self.name = name
        self.used_inputs = []
        self._result = {}
        self.attributes = {}
        
    def used_input(self,reference):
        self.used_inputs += [reference]
    
    @property
    def inputs(self):
        """
        returns inputs as a dictionary with the
        dependent's step identifier as key
        and a list of (output,index) tuples
        as value
        """
        return self.used_inputs
    
    @property
    def result(self):
        return self._result

class initstep(stepbase):
    def __init__(self,name, initdata = None):
        super(initstep,self).__init__(name)
        if initdata:
            self.attributes = initdata
    
    def __call__(self):
        self._result = self.attributes
        return self._result

    def s(self,**attributes):
        self.attributes = attributes
        return self
        
class yadagestep(stepbase):
    def __init__(self,name,spec,context):
        super(yadagestep,self).__init__(name)
        self.spec = spec
        self.context = context
        
    def __call__(self,**attributes):
        self.attributes.update(**attributes)
        self._result = packtivity(self.name,self.spec,self.attributes,self.context)
        return self._result
    
    def s(self,**attributes):
        self.attributes = attributes
        return self
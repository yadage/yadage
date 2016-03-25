from packtivity import packtivity

class initstep(object):
    def __init__(self,init_outputs):
        self.init_outputs = init_outputs

    @property
    def inputs(self):
        return {}
    
    @property
    def attributes(self):
        return {}
                
    def __call__(self):
        return self.init_outputs


class yadagestep(object):
    def __init__(self,name,spec,context):
        self.name = name
        self.spec = spec
        self.context = context
        self.attributes = {}
        self.used_inputs = {}
        self._result = {}
        
    def __repr__(self):
        return '<yadagestep name: {}>'.format(self.name)

    def used_input(self,reference):
        stepid,output,index = reference
        if stepid not in self.used_inputs:
            self.used_inputs[stepid] = []
        self.used_inputs[stepid].append((output,index))
    
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
    
    def __call__(self,**attributes):
        self.attributes.update(**attributes)
        self._result = packtivity(self.name,self.spec,self.attributes,self.context)
        return self._result
    
    def s(self,**attributes):
        self.attributes = attributes
        return self
from packtivity import packtivity

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

    def used_input(self,stepid,output,index):
        if stepid not in self.used_inputs:
            self.used_inputs[stepid] = []
        self.used_inputs[stepid].append((output,index))
    
    @property
    def inputs(self):
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
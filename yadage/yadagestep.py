from packtivity import packtivity

class yadagestep(object):
    def __init__(self,name,spec,context):
        self.name = name
        self.spec = spec
        self.context = context
        self.attributes = {}
        self.used_inputs = {}
        
    def __repr__(self):
        return '<yadagestep name: {}>'.format(self.name)

    def used_input(self,step,output,index):
        if step not in self.used_inputs:
            self.used_inputs[step] = []
        self.used_inputs[step].append((output,index))
    
    @property
    def inputs(self):
        return self.used_inputs
    
    def __call__(self,**attributes):
        return packtivity(self.name,self.spec,self.attributes,self.context)
    
    def s(self,**attributes):
        self.attributes = attributes
        return self
from yadagemodels import stage_base

class stage(stage_base):
    def __init__(self,name,workflow,dependencies,scheduler):
        super(stage,self).__init__(name,workflow,dependencies)
        self._scheduler = scheduler

    def schedule(self):
        return self._scheduler(self)
    
    @classmethod
    def fromfunc(cls,rules,after = None, name = None):
        def decorator(func):
            instance = cls(name or func.func_name, workflow, after or [],func)
            rules += [instance]
            return instance
        return decorator

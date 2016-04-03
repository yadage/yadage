class stage(object):
    def __init__(self,name,workflow):
        self.name = name
        self.workflow = workflow
        self.steps = []
        self.dag = None

    @property
    def context(self):
        return self.workflow.context

    def applicable(self,dag):
        for dep in self.dependencies:
            depstage = self.workflow.element(dep)
            if not depstage.steps:
                return False
            if not all([s.successful() for s in depstage.steps]):
                return False
        return True

    def apply(self,dag):
        """
        this is the binding to adage
        """
        self.dag = dag
        return self._rule(self)
    
    def addStep(self,step):
        dependencies = [self.dag.getNode(k) for k in step.inputs.keys()]
        node = self.dag.addTask(step, nodename = step.name, depends_on = dependencies)
        self.steps += [node]
        
    @classmethod
    def fromfunc(cls,wflow, name, after = []):
        instance = cls(name,wflow)
        instance.dependencies = after if type(after)==list else [after]
        wflow.add(name,instance)
        def decorator(func):
            instance._rule = func
            return instance
        return decorator

class workflow(object):
    def __init__(self):
        self.context = None
        self.elements = {}
    
    def walk(self, recurse = False):
        for k,v in self.elements.iteritems():
            if type(v)==stage:
                yield v
            if type(v)==workflow and recurse:
                for sub in v.walk(recurse):
                    yield sub
        
    
    def element(self,name):
        return self.elements[name]

    def add(self,name,element):
        """
        add stage or workflow
        """
        self.elements[name] = element



# print the_dag.getNode(wflow.element('init').steps[0])

# wflow.element('init').apply(dag)


# - one DAG
# - a nested structure of workflows
#     - top element is a workflow
#     - a workflow can have stages
#     - a workflow can have other workflows
# 
# - a stage is applied to the DAG
# - a stage needs to be able to access the workflow it is a part of
#   - so that it can select e.g. nodes from 
# - a stage needs to have access to the DAG to add nodes
# - a stage needs access to the context because packtivities need them
# - once we have the workflow, we can have a flattened list of stages (since each has a reference to its originating workflow)


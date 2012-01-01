import logging
import yaml
import adage
from yadage.yadagestep  import yadagestep, initstep
from yadage.yadagemodels import stage_base, jsonstage, YadageWorkflow, WorkflowView, STAGESEP

logging.basicConfig(level = logging.INFO)
stepdata = yaml.load(open('prototype_steps.yml'))

class setup_stage(stage_base):
    def __init__(self,yml,context):
        super(setup_stage,self).__init__(yml['name'],context,['init'])
        self.yml = yml
    def schedule(self):
        stepdata = self.yml['setup_steps']
        init = self.flowview.getSteps('init')
        for step in stepdata:
            s = initstep(step['name'],step['output'])
            for x in init:
                s.used_input(x.identifier,'',None)
            self.addStep(s)
        
setupdataA = {
    'name':'setupA',
    'setup_steps':[
        {'name': 'A1', 'output':{'outputA':'valA'}},
        {'name': 'A2', 'output':{'outputA':'valA'}},
    ]
}

setupdataB = {
    'name':'setupB',
    'setup_steps':[
        {'name': 'B1', 'output':{'outputB':['one','two','three']}},
        {'name': 'B2', 'output':{'outputB':['four','five','six']}}
    ]
}


rootcontext = {'workdir':''}
stageyaml = yaml.load(open('mcprodflow/rootflow.yml'))
rules = [setup_stage(x,rootcontext) for x in [setupdataA,setupdataB]]+[jsonstage(yml,rootcontext) for yml in stageyaml]
flow = YadageWorkflow.fromJSON(stageyaml,{'init':'data'},rootcontext)
adage.rundag(flow, track = False)



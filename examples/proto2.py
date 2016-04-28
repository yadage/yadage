import logging
import yaml
import adage
from yadage.yadagestep  import yadagestep, initstep, outputReference
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
                s.used_input(outputReference(x.identifier,'/'))
            self.addStep(s)
        
setupdataA = {
    'name':'setupA',
    'setup_steps':[
        {'name': 'A1', 'output':{'outputA':'valA_one'}},
        {'name': 'A2', 'output':{'outputA':'valA_two'}},
    ]
}

setupdataB = {
    'name':'setupB',
    'setup_steps':[
        {'name': 'B1', 'output':{'outputB':['a','b','c']}},
        {'name': 'B2', 'output':{'outputB':['1','2','3']}}
    ]
}

rootcontext = {'workdir':'/basework'}


rules = []
rules += [setup_stage(x,rootcontext) for x in [setupdataA,setupdataB]]
# rules += [jsonstage(yml,rootcontext) for yml in stageyaml]


from yadage.workflow_loader import loader


ld = loader('newschema')

initdata = {'par1': 0.1, 'par2': 0.3}
stageyaml = ld('map.yml')

rules += [jsonstage(yml,rootcontext) for yml in stageyaml]
flow = YadageWorkflow()
rootview = WorkflowView(flow)
rootview.addWorkflow(rules, initstep = initstep('init root', initdata))
adage.rundag(flow, track = True)



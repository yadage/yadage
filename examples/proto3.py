import logging
import yaml
import adage
from yadage.yadagestep  import yadagestep, initstep, outputReference
from yadage.yadagemodels import stage_base, jsonstage, YadageWorkflow, WorkflowView, STAGESEP
from yadage.workflow_loader import loader


logging.basicConfig(level = logging.INFO)

rootcontext = {'workdir':'/basework'}

ld = loader('mcprodflow')

initdata = {'par1': 0.1, 'par2': 0.3, 'nevents': 10000, 'seeds': [1,2,3,4]}
stageyaml = ld('rootflow.yml')

rules = [jsonstage(yml,rootcontext) for yml in stageyaml]
flow = YadageWorkflow()
rootview = WorkflowView(flow)
rootview.addWorkflow(rules, initstep = initstep('init root', initdata))
adage.rundag(flow, track = True)



import logging
import yaml
import json
import adage
from yadage.yadagestep      import yadagestep, initstep, outputReference
from yadage.yadagemodels    import stage_base, jsonstage, YadageWorkflow, WorkflowView, STAGESEP
from yadage.workflow_loader import loader, load_and_validate
import yadage
logging.basicConfig(level = logging.INFO)

schemadir = '/Users/lukas/Code/yadagedev/cap-schemas/schemas'
loadtoplevel = 'mcprodflowsub'


flowyaml = yadage.workflow_loader.workflow('rootflow.yml', toplevel = loadtoplevel, schemadir = schemadir)
rootcontext = {'workdir':'/basework'}
flow = YadageWorkflow.fromJSON(flowyaml,rootcontext)


flow.view().init({'par1': 0.1, 'par2': 0.3, 'nevents': 10000, 'seeds': [1,2,3]})
adage.rundag(flow, track = True)

with open('yadage.json','w') as dumpfile:
    json.dump(flow.stepsbystage,dumpfile)
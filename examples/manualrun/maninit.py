STATEFILE = 'manual_instance.json'


import yadage.backends.packtivity_celery
import yadage.backends.celeryapp
import yadage.workflow_loader
import yadage.yadagemodels
import os
import json

workflow_def = yadage.workflow_loader.workflow(
    toplevel = '../yadage-workflows/testing/busybox-flow',
    source = 'rootflow.yml'
)



rootcontext = {
    'readwrite': [os.path.abspath('manual')],
    'readonly': []
}

workflow = yadage.yadagemodels.YadageWorkflow.createFromJSON(workflow_def,rootcontext)

initdata = {}
workflow.view().init(initdata)


with open(STATEFILE,'w') as f:
    json.dump(workflow.json(),f)

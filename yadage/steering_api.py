#!/usr/bin/env python
import adage
import adage.backends
import logging
import os
import json
import workflow_loader
import yadage.packtivitybackend
from yadage.yadagemodels import YadageWorkflow
import visualize
log = logging.getLogger(__name__)

def run_workflow(workdir,analysis, initdata, loadtoplevel, loginterval, schemadir, validate = True, nparallel = 2):
    """
    Main entry point to run a Yadage workflow
    """
    
    log.info('running yadage workflow %s',analysis)
    if not os.path.exists(workdir):
        os.makedirs(workdir)
    
    backend = yadage.packtivitybackend.PacktivityBackend(nparallel)
    
    rootcontext = {
        'readwrite': [os.path.abspath(workdir)],
        'readonly': []
    }
    for k,v in initdata.iteritems():
        candpath = '{}/inputs/{}'.format(workdir,v)
        if os.path.exists(candpath):
            initdata[k] = '{}/inputs/{}'.format(rootcontext['readwrite'][0],v)
            
    workflow_json = workflow_loader.workflow(
        analysis,
        toplevel = loadtoplevel,
        schemadir = schemadir,
        validate = validate
    )
    workflow = YadageWorkflow.fromJSON(workflow_json,rootcontext)
    workflow.view().init(initdata)
    
    adage.rundag(workflow,
                 track = True,
                 backend = backend,
                 update_interval = 0.02,
                 trackevery = loginterval,
                 workdir = '{}/_adage'.format(workdir)
                )
    yadagedir = '{}/_yadage/'.format(workdir)
    os.makedirs(yadagedir)
    with open('{}/yadage.json'.format(yadagedir),'w') as f:
        json.dump(workflow.stepsbystage,f)
    with open('{}/yadage_template.json'.format(yadagedir),'w') as f:
        json.dump(workflow_json,f)
    
    visualize.write_prov_graph(yadagedir,workflow)
    log.info('finished yadage workflow %s',analysis)

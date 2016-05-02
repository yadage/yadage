#!/usr/bin/env python
import adage
import adage.backends
import yadagemodels
import logging
import os
import json
import workflow_loader
from yadage.yadagemodels import YadageWorkflow
import visualize
log = logging.getLogger(__name__)

def run_workflow(workdir,analysis, initdata, loadtoplevel, loginterval, schemadir, validate = True):
    """
    Main entry point to run a Yadage workflow
    """
    
    log.info('running yadage workflow %s',analysis)
    if not os.path.exists(workdir):
        raise RuntimeError('workdir %s does not exist',workdir)
    
    backend = adage.backends.MultiProcBackend(2)
    
    for k,v in initdata.iteritems():
        candpath = '{}/inputs/{}'.format(workdir,v)
        if os.path.exists(candpath):
            initdata[k] = '/workdir/inputs/{}'.format(v)
            
    workflow_json = workflow_loader.workflow(
        analysis,
        toplevel = loadtoplevel,
        schemadir = schemadir,
        validate = validate
    )
    
    rootcontext = {
        'readwrite': [os.path.abspath(workdir)],
        'readonly': []
    }
    
    workflow = YadageWorkflow.fromJSON(workflow_json,rootcontext)
    workflow.view().init(initdata)

    adage.rundag(workflow,
                 track = True,
                 backend = backend,
                 trackevery = loginterval,
                 workdir = '{}/_adage'.format(workdir)
                )
    os.makedirs('{}/_yadage/'.format(workdir))
    with open('{}/_yadage/yadage.json'.format(workdir),'w') as f:
        json.dump(workflow.stepsbystage,f)
    log.info('finished yadage workflow %s',analysis)
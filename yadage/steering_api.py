#!/usr/bin/env python
import adage
import adage.backends
import yadagemodels
import logging
import os
import workflow_loader
import visualize
log = logging.getLogger(__name__)

def run_workflow(workdir,analysis,initdata, loadtoplevel,loginterval,schemadir):
    """
    Main entry point to run a Yadage workflo
    """

    log.info('running yadage workflow %s',analysis)
    if not os.path.exists(workdir):
        raise RuntimeError('workdir %s does not exist',workdir)
    
    backend = adage.backends.MultiProcBackend(2)
    
    initdata.update(workdir = workdir)
    for k,v in initdata.iteritems():
        candpath = '{}/inputs/{}'.format(workdir,v)
        if os.path.exists(candpath):
            initdata[k] = '/workdir/inputs/{}'.format(v)
            
    workflow_json = workflow_loader.workflow(analysis, toplevel = loadtoplevel, schemadir = schemadir)

    context = {
        workdir: '/workdir'
    }
    
    workflow = yadagemodels.YadageWorkflow.fromJSON(workflow_json['stages'], initdata, context)
    # visualize.write_stage_graph(workdir,workflow)

    adage.rundag(workflow,
                 track = True,
                 backend = backend,
                 trackevery = loginterval,
                 workdir = workdir
                )
    
    # visualize.write_prov_graph(workdir,adageobj.dag,workflow)
    log.info('finished yadage workflow %s',analysis)
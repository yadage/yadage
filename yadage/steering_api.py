#!/usr/bin/env python
import adage
import adage.backends
import yadagemodels
import logging
import os
import workflow_loader
import visualize
log = logging.getLogger(__name__)

def run_workflow(workdir,analysis,context,loadtoplevel,loginterval,schemadir):
    """
    Main entry point to run a Yadage workflo
    """

    log.info('running yadage workflow %s',analysis)
    if not os.path.exists(workdir):
        raise RuntimeError('workdir %s does not exist',workdir)
    
    backend = adage.backends.MultiProcBackend(2)
    
    context.update(workdir = workdir)
    for k,v in context.iteritems():
        candpath = '{}/inputs/{}'.format(workdir,v)
        if os.path.exists(candpath):
            context[k] = '/workdir/inputs/{}'.format(v)
            
    workflow_json = workflow_loader.workflow(analysis, toplevel = loadtoplevel, schemadir = schemadir)

    workflow = yadagemodels.workflow.fromJSON(workflow_json,context)
    visualize.write_stage_graph(workdir,workflow)

    
    adageobj = adage.adageobject()
    adageobj.rules = workflow.stages.values()
    adage.rundag(adageobj,
                 track = True,
                 backend = backend,
                 trackevery = loginterval,
                 workdir = workdir
                )
    
    visualize.write_prov_graph(workdir,adageobj.dag,workflow)
    log.info('finished yadage workflow %s',analysis)
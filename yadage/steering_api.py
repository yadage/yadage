#!/usr/bin/env python
import adage
import adage.backends
import logging
import os
import json
import workflow_loader
from yadage.yadagemodels import YadageWorkflow
import visualize
import interactive


log = logging.getLogger(__name__)

def run_workflow(
    workdir,
    workflow,
    initdata,
    loadtoplevel,
    loginterval,
    schemadir,
    backend,
    user_interaction = False,
    validate = True
    ):
    """
    Main entry point to run a Yadage workflow
    """

    log.info('running yadage workflow %s',workflow)
    if not os.path.exists(workdir):
        os.makedirs(workdir)

    rootcontext = {
        'readwrite': [os.path.abspath(workdir)],
        'readonly': []
    }
    for k,v in initdata.iteritems():
        candpath = '{}/inputs/{}'.format(workdir,v)
        if os.path.exists(candpath):
            initdata[k] = '{}/inputs/{}'.format(rootcontext['readwrite'][0],v)

    workflow_json = workflow_loader.workflow(
        workflow,
        toplevel = loadtoplevel,
        schemadir = schemadir,
        validate = validate
    )
    workflow = YadageWorkflow.createFromJSON(workflow_json,rootcontext)
    workflow.view().init(initdata)

    yadagedir = '{}/_yadage/'.format(workdir)
    os.makedirs(yadagedir)

    with open('{}/yadage_instance_before.json'.format(yadagedir),'w') as f:
        json.dump(workflow.json(),f)

    if user_interaction:
        extend,submit = interactive.interactive_deciders()
        interactive_kwargs = {
            'extend_decider': extend,
            'submit_decider': submit
        }
    else:
        interactive_kwargs = {}

    adage.rundag(workflow,
                 track = True,
                 backend = backend,
                 update_interval = 0.02,
                 trackevery = loginterval,
                 workdir = '{}/_adage'.format(workdir),
                 **interactive_kwargs
                )

    with open('{}/yadage_instance.json'.format(yadagedir),'w') as f:
        json.dump(workflow.json(),f)
    with open('{}/yadage_template.json'.format(yadagedir),'w') as f:
        json.dump(workflow_json,f)

    # import IPython
    # IPython.embed()

    visualize.write_prov_graph(yadagedir,workflow)
    log.info('finished yadage workflow %s',workflow)

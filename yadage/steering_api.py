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
import  packtivity.statecontexts.poxisfs_context as statecontext


log = logging.getLogger(__name__)

RC_FAILED = 1
RC_SUCCEEDED = 0

def run_workflow(
    workdir,
    workflow,
    initdata,
    loadtoplevel,
    updateinterval,
    loginterval,
    schemadir,
    backend,
    user_interaction = False,
    validate = True,
    doviz = True
    ):
    """
    Main entry point to run a Yadage workflow
    """
    #let's be conservative and just assume we're going to fail. will set success RC explicityly
    return_value = RC_FAILED

    log.info('running yadage workflow %s',workflow)
    rootcontext = statecontext.make_new_context(workdir)

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

    try:
        adage.rundag(workflow,
                     default_trackers = doviz,
                     backend = backend,
                     update_interval = updateinterval,
                     trackevery = loginterval,
                     workdir = '{}/_adage'.format(workdir),
                     **interactive_kwargs
        )
        return_value = RC_SUCCEEDED
    except:
        log.exception('Unfortunately adage failed. :(')

    with open('{}/yadage_instance.json'.format(yadagedir),'w') as f:
        json.dump(workflow.json(),f)
    with open('{}/yadage_template.json'.format(yadagedir),'w') as f:
        json.dump(workflow_json,f)

    if doviz:
        visualize.write_prov_graph(yadagedir,workflow, vizformat = 'png')
        visualize.write_prov_graph(yadagedir,workflow, vizformat = 'pdf')
    log.info('finished yadage workflow %s, returning rc: %s',workflow,return_value)
    return return_value

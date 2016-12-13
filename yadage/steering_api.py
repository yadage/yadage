#!/usr/bin/env python
import adage
import adage.backends
import logging
import os
import json
import workflow_loader
from yadage.yadagemodels import YadageWorkflow
import visualize
import serialize
import interactive
import shutil
import packtivity.statecontexts.poxisfs_context as statecontext


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
    user_interaction=False,
    validate=True,
    doviz=True,
    accept_existing_workdir = False
):
    """
    Main entry point to run a Yadage workflow
    """
    # let's be conservative and just assume we're going to fail. will set
    # success RC explicityly
    return_value = RC_FAILED

    log.info('running yadage workflow %s', workflow)
    rootcontext = statecontext.make_new_context(workdir)
    
    yadagedir = '{}/_yadage/'.format(workdir)
    if os.path.exists(yadagedir):
        if not accept_existing_workdir:
            log.error('yadage meta directory exists. explicitly accept')
            return return_value
        log.info('yadage meta directory exists.. will remove and remake')
        shutil.rmtree(yadagedir)
    os.makedirs(yadagedir)


    for k, v in initdata.iteritems():
        candpath = '{}/init/{}'.format(workdir, v)
        if os.path.exists(candpath):
            initdata[k] = '{}/init/{}'.format(rootcontext['readwrite'][0], v)

    workflow_json = workflow_loader.workflow(
        workflow,
        toplevel=loadtoplevel,
        schemadir=schemadir,
        validate=validate
    )
    workflow = YadageWorkflow.createFromJSON(workflow_json, rootcontext)
    workflow.view().init(initdata)

    with open('{}/yadage_instance_before.json'.format(yadagedir), 'w') as f:
        json.dump(workflow.json(), f)

    if user_interaction:
        extend, submit = interactive.interactive_deciders()
        interactive_kwargs = {
            'extend_decider': extend,
            'submit_decider': submit
        }
    else:
        interactive_kwargs = {}

    try:
        adage.rundag(workflow,
                     default_trackers=doviz,
                     backend=backend,
                     update_interval=updateinterval,
                     trackevery=loginterval,
                     workdir='{}/_adage'.format(workdir),
                     **interactive_kwargs
                     )
        #only here will we explicitly set the RC to success...
        return_value = RC_SUCCEEDED
    except:
        log.exception('Unfortunately we failed. :(')

    serialize.snapshot(
        workflow,
        '{}/yadage_snapshot_workflow.json'.format(yadagedir),
        '{}/yadage_snapshot_backend.json'.format(yadagedir)
    )

    with open('{}/yadage_template.json'.format(yadagedir), 'w') as f:
        json.dump(workflow_json, f)

    if doviz:
        visualize.write_prov_graph(yadagedir, workflow, vizformat='png')
        visualize.write_prov_graph(yadagedir, workflow, vizformat='pdf')
    log.info('finished yadage workflow %s, returning rc: %s',
             workflow, return_value)
    return return_value

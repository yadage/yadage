import os
import yadageschemas
import json
import logging
import yadage.workflow_loader as workflow_loader
import yadage.utils as utils

from .wflowstate import load_model_fromstring
from .controllers import setup_controller
from .wflow import YadageWorkflow

log = logging.getLogger(__name__)

def create_workflow(
    metadir,
    workflow = None,
    initdata = None,
    toplevel = os.getcwd(),
    dataarg = None,
    dataopts = None,
    workflow_json = None,
    modelsetup = 'inmem',
    modelopts = None,
    controller = 'frommodel',
    ctrlopts = None,
    validate = True,
    schemadir = yadageschemas.schemadir):
    '''
    load workflow from spec and initialize it

    :param workflow: the workflow spec source
    :param toplevel: base URI against which to resolve JSON references in the spec
    :param initdata: initialization data for workflow

    prepares initial workflow object and returns controller
    '''

    rootprovider = utils.state_provider_from_string(dataarg, dataopts)

    if not workflow_json and not workflow:
        raise RuntimeError('need to provide either direct workflow spec or source to load from')

    if workflow_json:
        if validate: workflow_loader.validate(workflow_json)
    else:
        workflow_json = workflow_loader.workflow(
            workflow,
            toplevel=toplevel,
            schemadir=schemadir,
            validate=validate
        )

    with open('{}/yadage_template.json'.format(metadir), 'w') as f:
        json.dump(workflow_json, f)
    workflowobj = YadageWorkflow.createFromJSON(workflow_json, rootprovider)
    if initdata:
        log.info('initializing workflow with %s',initdata)
        workflowobj.view().init(initdata, rootprovider, discover = True)
    else:
        log.info('no initialization data')

    model = load_model_fromstring(modelsetup,modelopts,workflowobj)
    return setup_controller(
        model = model,
        controller = controller, ctrlopts = ctrlopts,
    )

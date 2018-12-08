import os
import yadageschemas
import json
import logging

from . import workflow_loader as workflow_loader

from .handlers.utils import handler_decorator
from .state_providers import state_provider_from_string
from .wflowstate import load_model_fromstring
from .controllers import setup_controller
from .wflow import YadageWorkflow

log = logging.getLogger(__name__)

handlers, creator = handler_decorator()


log = logging.getLogger(__name__)

@creator('local')
def local_workflows(
    metadir,
    workflow = None,
    initdata = None,
    toplevel = os.getcwd(),
    dataarg = None,
    dataopts = None,
    wflowopts = None,
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
    rootprovider = state_provider_from_string(dataarg, dataopts)

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
        wflowopts = wflowopts or {}
        discover = wflowopts.setdefault('discover',True)
        relative = wflowopts.setdefault('relative',True)
        log.info('initializing workflow with initdata: %s discover: %s relative: %s',initdata, discover, relative)
        workflowobj.view().init(
            initdata, rootprovider, discover = discover, relative = relative
        )
    else:
        log.info('no initialization data')

    model = load_model_fromstring(modelsetup,modelopts,workflowobj)
    return setup_controller(
        model = model,
        controller = controller, ctrlopts = ctrlopts,
    )

from contextlib import contextmanager
import logging
import importlib
import yadageschemas
import os

from .steering_object import YadageSteering
from .utils import setupbackend_fromstring
from .interactive import interactive_deciders

log = logging.getLogger(__name__)

def run_workflow(*args, **kwargs):
    '''
    convenience function around steering context, when no additional settings
    are desired.
    '''
    with steering_ctx(*args, **kwargs):
        pass

@contextmanager
def steering_ctx(
    dataarg,
    workflow = None,
    initdata = None,
    toplevel = os.getcwd(),
    backend = None,
    workflow_json = None,
    cache = None,
    dataopts = None,
    updateinterval = 0.02,
    loginterval = 30,
    schemadir = yadageschemas.schemadir,
    metadir = None,
    interactive=False,
    validate=True,
    visualize=True,
    accept_metadir = False,
    modelsetup = 'inmem',
    modelopts = None):
    '''
    context manage around yadage steering object.

    param dataarg: primary data argument (e.g. workdir) of the workflow
    param workflow: workflow spec source
    '''

    ys = YadageSteering()

    if cache:
        accept_metadir = True

    ys.prepare(
        dataarg = dataarg, dataopts = dataopts,
        metadir = metadir, accept_metadir = accept_metadir,
    )

    wflow_kwargs = dict() #if this stays empty, error will be raise by ys
    if workflow_json:
        wflow_kwargs = dict(workflow_json = workflow_json)
    elif workflow:
        wflow_kwargs = dict(workflow = workflow, toplevel = toplevel, validate = validate, schemadir = schemadir)

    ys.init_workflow(
        initdata = initdata,
        modelsetup = modelsetup,
        modelopts = modelopts,
        **wflow_kwargs
    )

    custom_tracker = os.environ.get('YADAGE_CUSTOM_TRACKER',None)
    if custom_tracker:
        modulename,trackerclassname = custom_tracker.split(':')
        module = importlib.import_module(modulename)
        trackerclass = getattr(module,trackerclassname)
        ys.adage_argument(additional_trackers = [trackerclass()])

    ys.adage_argument(
        default_trackers = visualize,
        trackevery = loginterval,
        update_interval = updateinterval,
    )
    if interactive:
        extend, submit = interactive_deciders()
        ys.adage_argument(
            extend_decider = extend,
            submit_decider = submit
        )

    yield ys

    backend = backend or setupbackend_fromstring('multiproc:auto')
    log.info('running yadage workflow %s on backend %s', workflow, backend)
    if cache:
        if cache == 'checksums':
            backend.enable_cache(':'.join([cache,os.path.join(ys.metadir,'cache.json')]))
        else:
            backend.enable_cache(cache)
    try:
        ys.run_adage(backend)
    finally:
        ys.serialize()
    if visualize:
        ys.visualize()
    

import importlib
import logging
import os
from contextlib import contextmanager

import yadageschemas

from .steering_object import YadageSteering
from .strategies import get_strategy

log = logging.getLogger(__name__)

def run_workflow(*args, **kwargs):
    '''
    convenience function around steering context, when no additional settings
    are desired.
    '''
    with steering_ctx(*args, **kwargs):
        pass

def execute_steering(
    steering_object,
    updateinterval = 0.02,
    loginterval = 30,
    default_trackers=True,
    strategy = None,
    backend = None,
    cache = None
    ):

    ys = steering_object
    ys.adage_argument(
        default_trackers = default_trackers,
        trackevery = loginterval,
        update_interval = updateinterval,
    )

    if cache:
        if cache == 'checksums':
            backend.enable_cache(':'.join([cache,os.path.join(ys.metadir,'cache.json')]))
        else:
            backend.enable_cache(cache)


    custom_tracker = os.environ.get('YADAGE_CUSTOM_TRACKER',None)
    if custom_tracker:
        modulename,trackerclassname = custom_tracker.split(':')
        module = importlib.import_module(modulename)
        trackerclass = getattr(module,trackerclassname)
        ys.adage_argument(additional_trackers = [trackerclass()])

    if strategy is not None:
        ys.adage_argument(**get_strategy(strategy))

    ys.run_adage(backend)

@contextmanager
def steering_ctx(
    dataarg,
    workflow = None,
    initdata = None,
    toplevel = os.getcwd(),
    backend = None,
    controller = 'frommodel',
    ctrlopts = None,
    workflow_json = None,
    cache = None,
    dataopts = None,
    updateinterval = 0.02,
    loginterval = 30,
    schemadir = yadageschemas.schemadir,
    metadir = None,
    strategy=None,
    validate=True,
    visualize=True,
    wflowopts = None,
    accept_metadir = False,
    modelsetup = 'inmem',
    modelopts = None
):

    ys = YadageSteering.create(
        metadir = metadir, accept_metadir = True if (accept_metadir or cache) else False,
        dataarg = dataarg, dataopts = dataopts, wflowopts = wflowopts,
        workflow_json = workflow_json,
        workflow = workflow, toplevel = toplevel,
        schemadir = schemadir, validate = validate,
        initdata = initdata,
        modelsetup = modelsetup, modelopts = modelopts,
        controller = controller, ctrlopts = ctrlopts,
    )

    yield ys

    try:
        execute_steering(
            steering_object = ys,
            updateinterval = updateinterval,
            loginterval = loginterval,
            default_trackers = visualize,
            strategy = strategy,
            backend = backend,
            cache = cache
        )
    finally:
        log.info('done. dumping workflow to disk.')
        ys.serialize()
    if visualize:
        log.info('visualizing workflow.')
        ys.visualize()

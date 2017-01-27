#!/usr/bin/env python

from contextlib import contextmanager
from steering_object import YadageSteering
import logging
import importlib
import interactive
import yadageschemas
import os
log = logging.getLogger(__name__)

RC_FAILED = 1
RC_SUCCEEDED = 0


def run_workflow(*args, **kwargs):
    """
    Main entry point to run a Yadage workflow
    """
    # let's be conservative and just assume we're going to fail. will set
    # success RC explicityly
    return_value = RC_FAILED
    try:
        with steering_ctx(*args, **kwargs):
            pass
        return_value = RC_SUCCEEDED
    except:
        log.exception('Unfortunately we failed. :(')
    return return_value

@contextmanager
def steering_ctx(
    workdir,
    workflow,
    initdata,
    loadtoplevel,
    backend,
    read = None,
    initdir = None,
    updateinterval = 0.02,
    loginterval = 30,
    schemadir = yadageschemas.schemadir,
    user_interaction=False,
    validate=True,
    doviz=True,
    accept_existing_workdir = False):
    
    log.info('running yadage workflow %s', workflow)
    ys = YadageSteering(logger = log)
    ys.prepare_workdir(workdir, accept_existing_workdir, contextinit = read)
    ys.init_workflow(workflow, loadtoplevel, initdata, initdir = initdir, validate = validate, schemadir = schemadir)


    custom_tracker = os.environ.get('YADAGE_CUSTOM_TRACKER',None)
    if custom_tracker:
        modulename,trackerclassname = custom_tracker.split(':')
        module = importlib.import_module(modulename)
        trackerclass = getattr(module,trackerclassname)
        ys.adage_argument(additional_trackers = [trackerclass()])

    ys.adage_argument(
        default_trackers = doviz,
        backend = backend,
        trackevery = loginterval,
        update_interval = updateinterval,
        workdir='{}/_adage'.format(ys.workdir)
    )
    if user_interaction:
        extend, submit = interactive.interactive_deciders()
        ys.adage_argument(
            extend_decider = extend,
            submit_decider = submit
        )
    yield ys
    ys.run_adage()
    ys.serialize()
    if doviz:
        ys.visualize()


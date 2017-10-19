#!/usr/bin/env python
import click
import os
import logging
import yadageschemas
import yadage.utils as utils
import yadage.steering_api as steering_api

log = logging.getLogger(__name__)

RC_FAILED = 1
RC_SUCCEEDED = 0


@click.command()
@click.argument('dataarg')
@click.argument('workflow')
@click.argument('initfiles', nargs=-1)
@click.option('-d', '--dataopt', multiple=True, default=None, help = 'options for the workflow data state')
@click.option('-p', '--parameter', multiple=True, help = '<parameter name>=<yaml string> input parameter specifcations ')
@click.option('-b', '--backend', default='multiproc:auto', help = 'packtivity backend string')
@click.option('-k', '--backendopt', multiple=True, default=None, help = 'options for the workflow data state')
@click.option('-c', '--cache', default='')
@click.option('-e', '--schemadir', default=yadageschemas.schemadir, help = 'schema directory for workflow validation')
@click.option('-i', '--loginterval', default=30, help = 'adage tracking interval in seconds')
@click.option('-s', '--modelsetup', default='inmem', help = 'wflow state model')
@click.option('-l', '--modelopt', multiple=True, default=None, help = 'options for the workflow state models')
@click.option('-m', '--metadir', default=None, help = 'directory to store workflow metadata')
@click.option('-t', '--toplevel', default=os.getcwd(), help = 'toplevel uri to be used to resolve workflow name and references from')
@click.option('-u', '--updateinterval', default=0.02, help = 'adage graph inspection interval in seconds')
@click.option('-v', '--verbosity', default='INFO', help = 'logging verbosity')
@click.option('--accept-metadir/--no-accept-metadir', default=False)
@click.option('--interactive/--not-interactive', default=False, help = 'en-/disable user interactio (sign-off graph extensions and packtivity submissions)')
@click.option('--validate/--no-validate', default=True, help = 'en-/disable workflow spec validation')
@click.option('--visualize/--no-visualize', default=False, help = 'visualize workflow graph')
def main(dataarg,
         workflow,
         initfiles,
         toplevel,
         verbosity,
         loginterval,
         updateinterval,
         schemadir,
         backend,
         dataopt,
         backendopt,
         interactive,
         modelsetup,
         modelopt,
         metadir,
         parameter,
         validate,
         visualize,
         cache,
         accept_metadir
         ):


    logging.basicConfig(level=getattr(logging, verbosity), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    initdata = utils.getinit_data(initfiles, parameter)
    dataopts = utils.options_from_eqdelimstring(dataopt)
    backendopts = utils.options_from_eqdelimstring(backendopt)
    modelopts = utils.options_from_eqdelimstring(modelopt)

    backend  = utils.setupbackend_fromstring(backend,backendopts)
    rc = RC_FAILED
    try:
        steering_api.run_workflow(
            workflow = workflow,
            toplevel = toplevel,
            validate = validate,
            schemadir = schemadir,

            initdata = initdata,

            backend = backend,
            cache = cache,

            dataarg = dataarg,
            dataopts = dataopts,
            metadir = metadir,
            accept_metadir = accept_metadir,
            modelsetup = modelsetup,
            modelopts = modelopts,
            updateinterval = updateinterval,
            loginterval = loginterval,
            visualize = visualize,
            interactive = interactive,
        )
        rc = RC_SUCCEEDED
    except:
        log.exception('workflow failed')
    if rc != RC_SUCCEEDED:
        exc = click.exceptions.ClickException(
            click.style("Workflow failed", fg='red')
        )
        exc.exit_code = rc
        raise exc

if __name__ == '__main__':
    main()

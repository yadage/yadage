#!/usr/bin/env python
import click
import os
import steering_api
import logging
import yadageschemas
import yaml
import clihelpers

log = logging.getLogger(__name__)


@click.command()
@click.option('-v', '--verbosity', default='INFO', help = 'logging verbosity')
@click.option('-i', '--loginterval', default=30, help = 'adage tracking interval in seconds')
@click.option('-u', '--updateinterval', default=0.02, help = 'adage graph inspection interval in seconds')
@click.option('-m', '--schemasource', default=yadageschemas.schemadir, help = 'schema directory for workflow validation')
@click.option('-b', '--backend', default='multiproc:auto', help = 'packtivity backend string')
@click.option('-c', '--cache', default='')
@click.option('-s', '--statectrl', default='inmem')
@click.option('-d','--initdir', default='init', help = "relative path (to workdir) to initialiation data directory")
@click.option('--interactive/--not-interactive', default=False, help = 'en-/disable user interactio (sign-off graph extensions and packtivity submissions)')
@click.option('--validate/--no-validate', default=True, help = 'en-/disable workflow spec validation')
@click.option('--accept-workdir/--no-accept-workdir', default=False)
# v below this we should only have options/arg available also in yadage-manual
@click.option('--visualize/--no-visualize', default=True, help = 'visualize workflow graph')
@click.option('-t', '--toplevel', default=os.getcwd(), help = 'toplevel uri to be used to resolve workflow name and references from')
@click.option('-r', '--read', default=None, help = 'YAML file to initialize the state context')
@click.option('-a', '--inputarchive', default=None, help = 'initial data to stage as input')
@click.option('--parameter', '-p', multiple=True, help = '<parameter name>=<yaml string> input parameter specifcations ')
@click.argument('workdir')
@click.argument('workflow')
@click.argument('initfiles', nargs=-1)
def main(workdir,
         workflow,
         initfiles,
         toplevel,
         verbosity,
         loginterval,
         updateinterval,
         schemasource,
         backend,
         interactive,
         parameter,
         validate,
         read,
         visualize,
         inputarchive,
         statectrl,
         cache,
         accept_workdir,
         initdir):

    logging.basicConfig(level=getattr(logging, verbosity), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if inputarchive:
        initdir = clihelpers.prepare_workdir_from_archive(workdir, inputarchive)
    else:
        initdir = os.path.join(workdir,initdir)

    initdata = clihelpers.getinit_data(initfiles, parameter)
    backend  = clihelpers.setupbackend_fromstring(backend, cacheconfig=cache)

    rc = steering_api.run_workflow(
        workdir,
        workflow,
        initdata,
        toplevel,
        backend = backend,
        initdir = initdir,
        updateinterval = updateinterval,
        loginterval = loginterval,
        read = yaml.load(open(read)) if read else None,
        validate = validate,
        doviz = visualize,
        schemadir = schemasource,
        user_interaction = interactive,
        accept_existing_workdir = accept_workdir,
        ctrlsetup = statectrl
    )
    if rc:
        exc = click.exceptions.ClickException(
            click.style("Workflow failed", fg='red'))
        exc.exit_code = rc
        raise exc
if __name__ == '__main__':
    main()

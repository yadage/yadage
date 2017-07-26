#!/usr/bin/env python
import click
import os
import steering_api
import logging
import yadageschemas
import yaml
import utils

log = logging.getLogger(__name__)

RC_FAILED = 1
RC_SUCCEEDED = 0


@click.command()
@click.argument('workdir')
@click.argument('workflow')
@click.argument('initfiles', nargs=-1)
@click.option('-p','--parameter', multiple=True, help = '<parameter name>=<yaml string> input parameter specifcations ')
@click.option('-b', '--backend', default='multiproc:auto', help = 'packtivity backend string')
@click.option('-t', '--toplevel', default=os.getcwd(), help = 'toplevel uri to be used to resolve workflow name and references from')
@click.option('-a', '--inputarchive', default=None, help = 'initial data to stage as input')
@click.option('-c', '--cache', default='')
@click.option('-v', '--verbosity', default='INFO', help = 'logging verbosity')
@click.option('-i', '--loginterval', default=30, help = 'adage tracking interval in seconds')
@click.option('-u', '--updateinterval', default=0.02, help = 'adage graph inspection interval in seconds')
@click.option('-s', '--statesetup', default='inmem', help = 'wflow state spec')
@click.option('-d','--initdir', default='init', help = "relative path (to workdir) to initialiation data directory")
@click.option('-r', '--read', default=None, help = 'YAML file to initialize the state context')
@click.option('--metadir', default=None, help = 'directory to store workflow metadata')
@click.option('--schemadir', default=yadageschemas.schemadir, help = 'schema directory for workflow validation')
@click.option('--interactive/--not-interactive', default=False, help = 'en-/disable user interactio (sign-off graph extensions and packtivity submissions)')
@click.option('--validate/--no-validate', default=True, help = 'en-/disable workflow spec validation')
@click.option('--accept-metadir/--no-accept-metadir', default=False)
@click.option('--visualize/--no-visualize', default=True, help = 'visualize workflow graph')
def main(workdir,
         workflow,
         initfiles,
         toplevel,
         verbosity,
         loginterval,
         updateinterval,
         schemadir,
         backend,
         interactive,
         metadir,
         parameter,
         validate,
         read,
         visualize,
         inputarchive,
         statesetup,
         cache,
         accept_metadir,
         initdir):

    logging.basicConfig(level=getattr(logging, verbosity), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    initdata = utils.getinit_data(initfiles, parameter)
    backend  = utils.setupbackend_fromstring(backend)
    read     = yaml.load(open(read)) if read else None

    rc = RC_FAILED
    try:
        steering_api.run_workflow(
            workdir = workdir,
            workflow = workflow,
            initdata = initdata,
            toplevel = toplevel,
            backend = backend,
            cache = cache,
            
            read = read,
            initdir = initdir,
            inputarchive = inputarchive,
            
            metadir = metadir,
            accept_metadir = accept_metadir,            
            
            statesetup = statesetup,
            
            updateinterval = updateinterval,
            loginterval = loginterval,
            validate = validate,
            visualize = visualize,
            schemadir = schemadir,
            interactive = interactive,
        )
        rc = RC_SUCCEEDED
    except:
        log.exception('workflow failed')
    if rc != RC_SUCCEEDED:
        exc = click.exceptions.ClickException(
            click.style("Workflow failed", fg='red'))
        exc.exit_code = rc
        raise exc

if __name__ == '__main__':
    main()

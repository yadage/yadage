#!/usr/bin/env python
import click
import os
import steering_api
import logging
import capschemas
import clihelpers

log = logging.getLogger(__name__)


@click.command()
@click.option('-v', '--verbosity', default='INFO')
@click.option('-i', '--loginterval', default=30)
@click.option('-u', '--updateinterval', default=0.02)
@click.option('-m', '--schemasource', default=capschemas.schemadir)
@click.option('-b', '--backend', default='multiproc:auto')
@click.option('-c', '--cache', default='')
@click.option('--interactive/--not-interactive', default=False)
@click.option('--validate/--no-validate', default=True)
@click.option('--accept-workdir/--no-accept-workdir', default=False)
# v below this we should only have options/arg available also in yadage-manual
@click.option('--visualize/--no-visualize', default=True)
@click.option('-t', '--toplevel', default=os.getcwd())
@click.option('-a', '--inputarchive', default=None)
@click.option('--parameter', '-p', multiple=True)
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
         visualize,
         inputarchive,
         cache,
         accept_workdir):
    logging.basicConfig(level=getattr(logging, verbosity))

    if inputarchive:
        clihelpers.prepare_workdir_from_archive(workdir, inputarchive)

    initdata = clihelpers.getinit_data(initfiles, parameter)
    backend = clihelpers.setupbackend_fromstring(backend, cacheconfig=cache)

    rc = steering_api.run_workflow(
        workdir,
        workflow,
        initdata,
        toplevel,
        updateinterval,
        loginterval,
        validate=validate,
        doviz=visualize,
        schemadir=schemasource,
        backend=backend,
        user_interaction=interactive,
        accept_existing_workdir = accept_workdir
    )
    if rc:
        exc = click.exceptions.ClickException(
            click.style("Workflow failed", fg='red'))
        exc.exit_code = rc
        raise exc

if __name__ == '__main__':
    main()

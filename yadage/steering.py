#!/usr/bin/env python
import click
import os
import steering_api
import logging
import yaml
import capschemas
import psutil
import zipfile
import urllib

log = logging.getLogger(__name__)

@click.command()
@click.argument('workdir')
@click.option('-t','--toplevel', default = os.getcwd())
@click.option('-v','--verbosity', default = 'INFO')
@click.option('-i','--loginterval', default = 30)
@click.option('-u','--updateinterval', default = 0.02)
@click.option('-c','--schemasource', default = capschemas.schemadir)
@click.option('-b','--backend', default = 'multiproc:auto')
@click.option('-a','--inputarchive', default = None)
@click.option('--interactive/--not-interactive', default = False)
@click.option('--validate/--no-validate', default = True)
@click.option('--parameter', '-p', multiple=True)
@click.option('--visualize/--no-visualize', default = True)
@click.argument('workflow')
@click.argument('initdatas', nargs = -1)
def main(workdir,
         workflow,
         initdatas,
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
         inputarchive):
    logging.basicConfig(level = getattr(logging,verbosity))

    if inputarchive:
        if os.path.exists(workdir):
            raise click.exceptions.ClickException(click.style("workdirectory exists and input archive give. Can't have both", fg = 'red'))
        inputdata = '{}/inputs'.format(workdir)
        os.makedirs(inputdata)
        localzipfile = '{}/inputarchive.zip'.format(workdir)
        urllib.urlretrieve(inputarchive,localzipfile)
        with zipfile.ZipFile(localzipfile) as zf:
            zf.extractall(path = inputdata)
        os.remove(localzipfile)

    initdata = {}
    for initfile in initdatas:
        initdata.update(**yaml.load(open(initfile)))

    for x in parameter:
        key,value = x.split('=')
        initdata[key]=yaml.load(value)

    if backend.startswith('multiproc'):
        import backends.packtivitybackend as pb
        nparallel  = backend.split(':')[1]
        if nparallel == 'auto':
            nparallel = psutil.cpu_count()
        else:
            nparallel = int(nparallel)
        backend = pb.PacktivityMultiProcBackend(nparallel)
    elif backend == 'celery':
        import backends.celeryapp
        import backends.packtivity_celery as pc
        backend = pc.PacktivityCeleryBackend(backends.celeryapp.app)
    elif backend == 'foreground':
        import backends.packtivitybackend as pb
        backend = pb.PacktivityForegroundBackend()
    elif backend == 'jira':
        import backends.jira as jb
        backend = jb.JiraBackend('workflow request - {}:{}'.format(toplevel,workflow),'some description')

    rc =  steering_api.run_workflow(
            workdir,
            workflow,
            initdata,
            toplevel,
            updateinterval,
            loginterval,
            validate = validate,
            doviz = visualize,
            schemadir = schemasource,
            backend = backend,
            user_interaction = interactive)
    if rc:
        exc = click.exceptions.ClickException(click.style("Workflow failed", fg = 'red'))
        exc.exit_code = rc
        raise exc

if __name__ == '__main__':
    main()

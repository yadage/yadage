#!/usr/bin/env python
import click
import os
import steering_api
import logging
import yaml
import capschemas

log = logging.getLogger(__name__)

@click.command()
@click.argument('workdir')
@click.option('-t','--toplevel', default = os.getcwd())
@click.option('-v','--verbosity', default = 'INFO')
@click.option('-i','--loginterval', default = 30)
@click.option('-c','--schemasource', default = capschemas.schemadir)
@click.option('-b','--backend', default = 'multiproc:2')
@click.option('--interactive/--not-interactive', default = False)
@click.option('--validate/--no-validate', default = True)
@click.option('--parameter', '-p', multiple=True)
@click.argument('workflow')
@click.argument('initdata', default = '')
def main(workdir,workflow,initdata,toplevel,verbosity,loginterval,schemasource,backend,interactive,parameter,validate):
    logging.basicConfig(level = getattr(logging,verbosity))
    initdata = yaml.load(open(initdata)) if initdata else {}

    for x in parameter:
        key,value = x.split('=')
        initdata[key]=value

    if backend.startswith('multiproc'):
        import backends.packtivitybackend as pb
        nparallel = int(backend.split(':')[1])
        backend = pb.PacktivityMultiProcBackend(nparallel)
    elif backend == 'celery':
        import backends.celeryapp
        import backends.packtivity_celery as pc
        backend = pc.PacktivityCeleryBackend(backends.celeryapp.app)
    elif backend == 'foreground':
        import backends.packtivitybackend as pb
        backend = pb.PacktivityForegroundBackend()

    steering_api.run_workflow(
        workdir,
        workflow,
        initdata,
        toplevel,
        loginterval,
        validate = validate,
        schemadir = schemasource, backend = backend, user_interaction = interactive)

if __name__ == '__main__':
    main()

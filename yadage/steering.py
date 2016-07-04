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
@click.option('-p','--parallel', default = 2)
@click.argument('workflow')
@click.argument('initdata', default = '')
def main(workdir,workflow,initdata,toplevel,verbosity,loginterval,schemasource,parallel):
    logging.basicConfig(level = getattr(logging,verbosity))
    initdata = yaml.load(open(initdata)) if initdata else {}
    steering_api.run_workflow(workdir,workflow,initdata,toplevel,loginterval,schemadir = schemasource, nparallel = parallel)

if __name__ == '__main__':
    main()

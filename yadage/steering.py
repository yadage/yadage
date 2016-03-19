#!/usr/bin/env python
import click
import os
import steering_api
import logging
import yaml

log = logging.getLogger(__name__)

@click.command()
@click.argument('workdir')
@click.option('-t','--toplevel', default = os.getcwd())
@click.option('-v','--verbosity', default = 'INFO')
@click.option('-i','--loginterval', default = 30)
@click.option('-c','--schemasource', default = 'from-github')
@click.argument('analysis')
@click.argument('global_context', default = {})
def main(workdir,analysis,global_context,toplevel,verbosity,loginterval,schemasource):
    logging.basicConfig(level = getattr(logging,verbosity))
    context = yaml.load(open(global_context)) if global_context else {}
    steering_api.run_workflow(workdir,analysis,context,toplevel,loginterval,schemadir = schemasource)

if __name__ == '__main__':
    main()


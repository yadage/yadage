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
@click.argument('analysis')
@click.argument('global_context')
def main(workdir,analysis,global_context,toplevel,verbosity,loginterval):
    logging.basicConfig(level = getattr(logging,verbosity))
    context = yaml.load(open(global_context))
    steering_api.run_workflow(workdir,analysis,context,toplevel,loginterval)

if __name__ == '__main__':
  main()


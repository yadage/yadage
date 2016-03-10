#!/usr/bin/env python
import click
import os
import steering_api
import logging

logging.basicConfig(level = logging.INFO)
log = logging.getLogger(__name__)

@click.command()
@click.argument('workdir')
@click.option('-g','--globalwork', default = None)
@click.option('-t','--toplevel', default = os.getcwd())
@click.argument('analysis')
@click.argument('global_context')
def main(workdir,globalwork,analysis,global_context,toplevel):
    toplevel = os.path.abspath(toplevel)
    steering_api.run_workflow(workdir,globalwork if globalwork else workdir,analysis,global_context,toplevel)

if __name__ == '__main__':
  main()


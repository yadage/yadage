#!/usr/bin/env python

import click
import json
import os
# import logging
# logging.basicConfig(level = logging.INFO)

import yadage.backends.packtivity_celery
import yadage.backends.celeryapp
import yadage.yadagemodels
import yadage.visualize
import yadage.interactive
import adage
import adage.visualize

@click.command()
@click.argument('statefile')
def main(statefile):
    backend = yadage.backends.packtivity_celery.PacktivityCeleryBackend(
        yadage.backends.celeryapp.app
    )

    workflow = yadage.yadagemodels.YadageWorkflow.fromJSON(
        json.load(open(statefile)),
        yadage.backends.packtivity_celery.PacktivityCeleryProxy,
        backend
    )

    extend_decider,submit_decider = yadage.interactive.interactive_deciders()
    coroutine = adage.adage_coroutine(backend,extend_decider,submit_decider)
    coroutine.next() #prime the coroutine....
    coroutine.send(workflow)

    try:
        click.secho('stepping workflow', fg = 'green')
        coroutine.next()
        click.secho('saving new state', fg = 'green')
    except StopIteration:
        click.secho('workflow done.', fg = 'green')
        yadage.visualize.write_prov_graph(os.getcwd(),workflow)
        adage.visualize.print_dag(workflow.dag,'adage',os.getcwd(),1.0)

    with open(statefile,'w') as f:
        json.dump(workflow.json(),f)

if __name__ == '__main__':
    main()

#!/usr/bin/env python

import click
import json
import os
import logging
logging.basicConfig(level = logging.INFO)

import yadage.backends.packtivity_celery
import yadage.backends.celeryapp
import yadage.yadagemodels
import yadage.visualize
import adage
import adage.visualize

def decide_rule(rule,state):
    click.secho('we could extend DAG with rule: {}. current state: {}'.format(rule,state), fg = 'blue')
    shall = raw_input(click.style("Shall we? (y/N) ", fg = 'blue')).lower() == 'y'
    if shall:
        click.secho('ok we will extend.', fg = 'green')
    else:
        click.secho('maybe another time...', fg = 'yellow')
    return shall

def decide_step(dag,nodeobj):
    print 'we could submit a DAG node (id: {}) DAG is: {}'.format(nodeobj,dag)
    shall = raw_input(click.style("Shall we? (y/N) ", fg = 'magenta')).lower() == 'y'
    if shall:
        click.secho('ok we will submit.', fg = 'green')
    else:
        click.secho('will not submit for now...', fg = 'yellow')
    return shall

def custom_decider(decide_func):
    # we yield until we receive some data via send()
    def decider():
        data = yield
        while True:
            data = yield decide_func(*data)
    return decider

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

    extend_decider = custom_decider(decide_rule)()
    extend_decider.next() #prime decider

    submit_decider = custom_decider(decide_step)()
    submit_decider.next() #prime decider

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

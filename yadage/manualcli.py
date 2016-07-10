#!/usr/bin/env python

import os
import json
import yaml
import click

import adage
import adage.visualize

import yadage.backends.packtivity_celery
import yadage.backends.celeryapp
import yadage.workflow_loader
import yadage.yadagemodels
import yadage.visualize
import logging
logging.basicConfig()

@click.group()
def mancli():
    pass

@mancli.command()
@click.argument('workdir')
@click.argument('workflow')
@click.argument('initdata', default = '')
@click.option('-t','--toplevel', default = os.getcwd())
@click.option('-s','--statefile', default = 'manual_instance.json')
def init(workdir,workflow,initdata,statefile,toplevel):
    workflow_def = yadage.workflow_loader.workflow(
        toplevel = toplevel,
        source = workflow
    )

    rootcontext = {
        'readwrite': [os.path.abspath(workdir)],
        'readonly': []
    }

    workflow = yadage.yadagemodels.YadageWorkflow.createFromJSON(workflow_def,rootcontext)

    initdata = yaml.load(open(initdata)) if initdata else {}
    workflow.view().init(initdata)

    with open(statefile,'w') as f:
        json.dump(workflow.json(),f)

    click.secho('initialized workflow', fg = 'green')



def decide_rule(rule,state):
    click.secho('we could extend DAG with rule', fg = 'blue')
    click.secho('rule: {}/{} ({})'.format(rule.offset,rule.rule.name,rule.identifier))
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

@mancli.command()
@click.option('-s','--statefile', default = 'manual_instance.json')
def step(statefile):
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
        click.secho('try stepping workflow')
        coroutine.next()
    except StopIteration:
        click.secho('workflow done.', fg = 'green')
        yadage.visualize.write_prov_graph(os.getcwd(),workflow)
        adage.visualize.print_dag(workflow.dag,'adage',os.getcwd(),1.0)

    with open(statefile,'w') as f:
        json.dump(workflow.json(),f)


if __name__ == '__main__':
    mancli()

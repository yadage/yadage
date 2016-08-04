#!/usr/bin/env python

import os
import json
import yaml
import click
import time

import adage
import adage.visualize as av

import packtivity.statecontexts.poxisfs_context as statecontext

import yadage.backends.packtivity_celery
import yadage.backends.celeryapp
import yadage.workflow_loader
import yadage.yadagemodels
import yadage.visualize
import yadage.interactive
import logging
log = logging.getLogger(__name__)

@click.group()
def mancli():
    pass

@mancli.command()
@click.argument('workdir')
@click.argument('workflow')
@click.argument('initdata', default = '')
@click.option('-t','--toplevel', default = os.getcwd())
@click.option('-s','--statefile', default = 'yadage_instance.json')
def init(workdir,workflow,initdata,statefile,toplevel):
    workflow_def = yadage.workflow_loader.workflow(
        toplevel = toplevel,
        source = workflow
    )

    rootcontext = statecontext.make_new_context(workdir)

    workflow = yadage.yadagemodels.YadageWorkflow.createFromJSON(workflow_def,rootcontext)

    initdata = yaml.load(open(initdata)) if initdata else {}
    workflow.view().init(initdata)


    click.secho('initialized workflow', fg = 'green')

    yadagedir = get_yadagedir(workdir)
    '{}/_yadage'.format(workdir)
    os.makedirs(yadagedir)
    statefile = '{}/{}'.format(yadagedir,statefile)
    click.secho('statefile at {}'.format(statefile))
    with open(statefile,'w') as f:
        json.dump(workflow.json(),f)



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

def get_yadagedir(workdir):
    return '{}/_yadage'.format(workdir)

def finalize_manual(workdir,workflow):
    click.secho('workflow done.', fg = 'green')
    yadage.visualize.write_prov_graph(get_yadagedir(workdir),workflow)

def load_state(statefile):
    backend = yadage.backends.packtivity_celery.PacktivityCeleryBackend(
        yadage.backends.celeryapp.app
    )

    click.secho('loading state from {}'.format(statefile))
    workflow = yadage.yadagemodels.YadageWorkflow.fromJSON(
        json.load(open(statefile)),
        yadage.backends.packtivity_celery.PacktivityCeleryProxy,
        backend
    )
    return backend,workflow

@mancli.command()
@click.argument('workdir')
@click.option('-s','--statefile', default = 'yadage_instance.json')
@click.option('-v','--verbosity', default = 'ERROR')
def step(workdir,statefile,verbosity):
    logging.basicConfig(level = getattr(logging,verbosity))
    statefile = '{}/{}'.format(get_yadagedir(workdir),statefile)
    backend, workflow = load_state(statefile)

    extend_decider,submit_decider = yadage.interactive.interactive_deciders()
    coroutine = adage.adage_coroutine(backend,extend_decider,submit_decider)
    coroutine.next() #prime the coroutine....

    coroutine.send(workflow)
    try:
        click.secho('try stepping workflow')
        coroutine.next()
    except StopIteration:
        finalize_manual(workdir,workflow)

    with open(statefile,'w') as f:
        json.dump(workflow.json(),f)
    av.save_dot(av.colorize_graph_at_time(workflow.dag,time.time()).to_string(),'{}/{}'.format(get_yadagedir(workdir),'adage.png'),'png')

import reset as yr
@mancli.command()
@click.argument('workdir')
@click.argument('name')
@click.option('-s','--statefile', default = 'yadage_instance.json')
@click.option('-o','--offset', default = '')
def reset(workdir,statefile,offset,name):
    statefile = '{}/{}'.format(get_yadagedir(workdir),statefile)
    backend, workflow = load_state(statefile)
    yr.reset_state(workflow,offset,name)
    with open(statefile,'w') as f:
        json.dump(workflow.json(),f)
    av.save_dot(av.colorize_graph_at_time(workflow.dag,time.time()).to_string(),'{}/{}'.format(get_yadagedir(workdir),'adage.png'),'png')

if __name__ == '__main__':
    mancli()

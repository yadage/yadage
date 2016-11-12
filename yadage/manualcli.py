#!/usr/bin/env python
import os
import json
import click
import time
import adage
import yadage.backends.packtivity_celery
import yadage.backends.celeryapp
import yadage.workflow_loader
import yadage.yadagemodels
import yadage.visualize
import yadage.interactive
import clihelpers
import logging
import serialize

import adage.visualize as av
import packtivity.statecontexts.poxisfs_context as statecontext
import reset as yr

from contextlib import contextmanager

log = logging.getLogger(__name__)

@contextmanager
def workflowctx(workdir,statefile,backendfile):
    statefile = '{}/{}'.format(get_yadagedir(workdir),statefile)
    backendfile = '{}/{}'.format(get_yadagedir(workdir),backendfile)
    backend, workflow =  load_state(statefile)

    yield backend, workflow

    serialize.snapshot(
        workflow,
        statefile,
        backendfile
    )
    av.save_dot(
        av.colorize_graph_at_time(workflow.dag,time.time()).to_string(),
        '{}/{}'.format(get_yadagedir(workdir),
        'adage.png'),
        'png'
    )

@click.group()
def mancli():
    pass

@mancli.command()
@click.option('-s','--statefile', default = 'yadage_wflow_state.json')
@click.option('-b','--backendfile', default = 'yadage_backend_state.json')
@click.option('-t','--toplevel', default = os.getcwd())
@click.option('-a','--inputarchive', default = None)
@click.option('--parameter', '-p', multiple=True)
@click.argument('workdir')
@click.argument('workflow')
@click.argument('initfiles', default = '')
def init(workdir,workflow,initfiles,statefile,backendfile,toplevel,parameter,inputarchive):
    workflow_def = yadage.workflow_loader.workflow(
        toplevel = toplevel,
        source = workflow
    )

    rootcontext = statecontext.make_new_context(workdir)
    workflow = yadage.yadagemodels.YadageWorkflow.createFromJSON(workflow_def,rootcontext)

    initdata = clihelpers.getinit_data(initfiles,parameter)
    workflow.view().init(initdata)

    click.secho('initialized workflow', fg = 'green')

    yadagedir = get_yadagedir(workdir)
    '{}/_yadage'.format(workdir)
    os.makedirs(yadagedir)


    statefile = '{}/{}'.format(get_yadagedir(workdir),statefile)
    backendfile = '{}/{}'.format(get_yadagedir(workdir),backendfile)

    click.secho('statefile at {}'.format(statefile))
    serialize.snapshot(
        workflow,
        statefile,
        backendfile
    )

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
@click.argument('name')
@click.option('-o','--offset',default = '')
@click.option('-s','--statefile', default = 'yadage_wflow_state.json')
@click.option('-b','--backendfile', default = 'yadage_backend_state.json')
@click.option('-v','--verbosity', default = 'ERROR')
def apply(workdir,name,offset,statefile,backendfile,verbosity):
    logging.basicConfig(level = getattr(logging,verbosity))
    with workflowctx(workdir,statefile,backendfile) as (backend, workflow):
        rule = workflow.view(offset).getRule(name)
        if not rule:
            click.secho('No such rule, pick one of the below:', fg = 'red')
            for x in workflow.rules:
                click.secho('{}/{}'.format(x.offset,x.rule.name))
            return
        if not rule.applicable(workflow):
            click.secho('Rule is not applicable.', fg = 'red')
            return
        workflow.rules.remove(rule)
        rule.apply(workflow)
        workflow.applied_rules.append(rule)

@mancli.command()
@click.argument('workdir')
@click.option('-s','--statefile', default = 'yadage_wflow_state.json')
@click.option('-b','--backendfile', default = 'yadage_backend_state.json')
@click.option('-v','--verbosity', default = 'ERROR')
def step(workdir,statefile,backendfile,verbosity):
    logging.basicConfig(level = getattr(logging,verbosity))
    with workflowctx(workdir,statefile,backendfile) as (backend, workflow):
        extend_decider,submit_decider = yadage.interactive.interactive_deciders()
        coroutine = adage.adage_coroutine(backend,extend_decider,submit_decider)
        coroutine.next() #prime the coroutine....
        coroutine.send(workflow)
        try:
            click.secho('try stepping workflow')
            coroutine.next()
        except StopIteration:
            finalize_manual(workdir,workflow)

@mancli.command()
@click.argument('workdir')
@click.argument('name')
@click.option('-s','--statefile', default = 'yadage_wflow_state.json')
@click.option('-b','--backendfile', default = 'yadage_backend_state.json')
@click.option('-o','--offset', default = '')
def reset(workdir,statefile,backendfile,offset,name):
    with workflowctx(workdir,statefile,backendfile) as (backend, workflow):
        yr.reset_state(workflow,offset,name)

if __name__ == '__main__':
    mancli()

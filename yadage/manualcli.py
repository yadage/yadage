#!/usr/bin/env python
import logging
import click
import os
import manualutils
import yadage.workflow_loader
import packtivity.statecontexts.poxisfs_context as statecontext
import clihelpers
import serialize
import adage
import reset as yr

log = logging.getLogger(__name__)


@click.group()
def mancli():
    pass


@mancli.command()
@click.option('-s', '--statefile', default='yadage_wflow_state.json')
@click.option('-b', '--backendfile', default='yadage_backend_state.json')
@click.option('-t', '--toplevel', default=os.getcwd())
@click.option('-a', '--inputarchive', default=None)
@click.option('--parameter', '-p', multiple=True)
@click.argument('workdir')
@click.argument('workflow')
@click.argument('initfiles', default='')
def init(workdir, workflow, initfiles, statefile, backendfile, toplevel, parameter, inputarchive):
    workflow_def = yadage.workflow_loader.workflow(
        toplevel=toplevel,
        source=workflow
    )

    rootcontext = statecontext.make_new_context(workdir)
    workflow = yadage.yadagemodels.YadageWorkflow.createFromJSON(
        workflow_def, rootcontext)

    initdata = clihelpers.getinit_data(initfiles, parameter)
    workflow.view().init(initdata)

    click.secho('initialized workflow', fg='green')

    yadagedir = manualutils.get_yadagedir(workdir)
    os.makedirs(yadagedir)

    statefile = '{}/{}'.format(manualutils.get_yadagedir(workdir), statefile)
    backendfile = '{}/{}'.format(manualutils.get_yadagedir(workdir),
                                 backendfile)

    click.secho('statefile at {}'.format(statefile))
    serialize.snapshot(
        workflow,
        statefile,
        backendfile
    )


@mancli.command()
@click.argument('workdir')
@click.argument('name', default='')
@click.option('-o', '--offset', default='')
@click.option('-s', '--statefile', default='yadage_wflow_state.json')
@click.option('-b', '--backendfile', default='yadage_backend_state.json')
@click.option('-v', '--verbosity', default='ERROR')
def apply(workdir, name, offset, statefile, backendfile, verbosity):
    logging.basicConfig(level=getattr(logging, verbosity))
    with manualutils.workflowctx(workdir, statefile, backendfile) as (backend, workflow):
        if not name:
            click.secho('Applicable Rules: ', fg='blue')
            for x in manualutils.applicable_rules(workflow):
                click.secho('{}/{}'.format(x.offset, x.rule.name))
            return
        rule = workflow.view(offset).getRule(name)
        if not rule:
            click.secho(
                'No such rule, pick one of the applicable below:', fg='red')
            for x in manualutils.applicable_rules(workflow):
                click.secho('{}/{}'.format(x.offset, x.rule.name))
            return
        if not rule.applicable(workflow):
            click.secho('Rule is not applicable.', fg='red')
            return
        workflow.rules.remove(rule)
        rule.apply(workflow)
        workflow.applied_rules.append(rule)


@mancli.command()
@click.argument('workdir')
@click.argument('name')
@click.option('-o', '--offset', default='')
@click.option('-s', '--statefile', default='yadage_wflow_state.json')
@click.option('-b', '--backendfile', default='yadage_backend_state.json')
@click.option('-v', '--verbosity', default='ERROR')
def preview(workdir, name, offset, statefile, backendfile, verbosity):
    with manualutils.workflowctx(workdir, statefile, backendfile) as (backend, workflow):
        new_rules, new_nodes = manualutils.preview_rule(workflow, name, offset)
        click.secho('Preview of Stage: # new rules: {} # new nodes {}'.format(
            len(new_rules), len(new_nodes)))
        for n in new_nodes:
            click.secho(
                '-> new node "{}" with {} upstream dependencies'.format(n['name'], len(n['parents'])))


@mancli.command()
@click.argument('workdir')
@click.option('-s', '--statefile', default='yadage_wflow_state.json')
@click.option('-b', '--backendfile', default='yadage_backend_state.json')
@click.option('-v', '--verbosity', default='ERROR')
def step(workdir, statefile, backendfile, verbosity):
    logging.basicConfig(level=getattr(logging, verbosity))
    with manualutils.workflowctx(workdir, statefile, backendfile) as (backend, workflow):
        extend_decider, submit_decider = yadage.interactive.interactive_deciders()
        coroutine = adage.adage_coroutine(
            backend, extend_decider, submit_decider)
        coroutine.next()  # prime the coroutine....
        coroutine.send(workflow)
        try:
            click.secho('try stepping workflow')
            coroutine.next()
        except StopIteration:
            manualutils.finalize_manual(workdir, workflow)


@mancli.command()
@click.argument('workdir')
@click.argument('name')
@click.option('-s', '--statefile', default='yadage_wflow_state.json')
@click.option('-b', '--backendfile', default='yadage_backend_state.json')
@click.option('-o', '--offset', default='')
def reset(workdir, statefile, backendfile, offset, name):
    with manualutils.workflowctx(workdir, statefile, backendfile) as (backend, workflow):
        yr.reset_state(workflow, offset, name)

if __name__ == '__main__':
    mancli()

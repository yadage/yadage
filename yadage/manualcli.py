#!/usr/bin/env python
import logging
import click
import functools
import os
from packtivity.statecontexts.posixfs_context import LocalFSState
from yadage.state_providers.localposix import LocalFSProvider

from .steering_object import YadageSteering
from .steering_api import execute_steering
from .stages import JsonStage
import yadage.manualutils as manualutils
import yadage.utils as utils
import yadage.reset as reset_module
import yadage.workflow_loader as workflow_loader

log = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)


def connection_options(func):
    @click.option('-m', '--metadir', default='yadagemeta', help = 'directory to store workflow metadata')
    @click.option('--accept-metadir/--no-accept-metadir', default=True)
    @click.option('-r', '--controller', default='frommodel')
    @click.option('-o', '--ctrlopt', multiple=True, default=None, help = 'options for the workflow controller')
    @click.option('-s', '--modelsetup', default='filebacked:yadage_state.json')
    @click.option('-l', '--modelopt', multiple=True, default=None, help = 'options for the workflow state models')
    @click.option('-b', '--backend', default='celery')
    @click.option('--local/--remote', default = True)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

def common_options(func):
    @click.option('-v', '--verbosity', default='ERROR')
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@click.group()
def mancli():
    pass

@mancli.command()
@click.option('-s', '--modelsetup', default='filebacked:yadage_state.json')
@click.option('-t', '--toplevel', default=os.getcwd())
@click.option('-d', '--dataopt', multiple=True, default=None, help = 'options for the workflow data state')
@click.option('--metadir', default=None, help = 'directory to store workflow metadata')
@click.option('--parameter', '-p', multiple=True)
@click.argument('workdir')
@click.argument('workflow')
@click.argument('initfiles', nargs = -1)
def init(workdir, workflow, initfiles, modelsetup, dataopt, metadir, toplevel, parameter):
    initdata = utils.getinit_data(initfiles, parameter)
    dataopts = utils.options_from_eqdelimstring(dataopt)

    ys = YadageSteering.create(
        dataarg = workdir,
        dataopts = dataopts,
        workflow = workflow,
        toplevel = toplevel,
        initdata = initdata,
        metadir = metadir,
        modelsetup = modelsetup,
    )
    assert ys

def click_print_applicable_stages(controller):
    click.secho('Applicable Stages: ', fg='blue')
    for x in controller.adageobj.rules:
        if x.identifier in controller.applicable_rules():
            click.secho('{}/{}'.format(x.offset, x.rule.name))

def click_print_submittable_nodes(controller):
    click.secho('Submittable Nodes: ', fg='blue')
    _, s2r = manualutils.rule_steps_indices(controller.adageobj)
    for x in controller.adageobj.dag.nodes():
        node = controller.adageobj.dag.getNode(x)
        rule = controller.adageobj.view().getRule(identifier = s2r[node.identifier])
        if node.identifier in controller.submittable_nodes():
            click.secho('node: {} ({}) part of stage {}'.format(node.name, node.identifier,  '/'.join([rule.offset,rule.rule.name])))

@mancli.command()
@click.option('-n','--name', default=None)
@connection_options
@common_options
def apply(name,
          metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
          verbosity
         ):

    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    controller = ys.controller

    if not name:
        click_print_applicable_stages(controller)
        return

    offset, name = name.rsplit('/',1)
    rule = controller.adageobj.view(offset).getRule(name)
    if not rule:
        click.secho('No such stage, pick one of the applicable below:', fg='red')
        click_print_applicable_stages(controller)
        return

    if rule in controller.adageobj.applied_rules:
        click.secho('Already applied.', fg = 'yellow')
        return

    if rule.identifier not in controller.applicable_rules():
        click.secho('Rule not yet appilcable', fg = 'red')
        return

    controller.apply_rules([rule.identifier])
    click.secho('stage applied', fg = 'green')

@mancli.command()
@click.option('-n','--nodeid', default=None)
@click.option('-a','--allof', default=None)
@click.option('-o', '--offset', default='')
@connection_options
@common_options
def submit(nodeid, allof, offset,
           metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
           verbosity
           ):
    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    controller = ys.controller

    if not (allof or nodeid):
        click_print_submittable_nodes(controller)
        return

    if nodeid:
        nodes_to_submit = [nodeid] if nodeid in controller.submittable_nodes() else []
    if allof:
        offset, name = allof.rsplit('/',1)
        rule = controller.adageobj.view().getRule(name = name, offset = offset)
        if not rule:
            click.secho('stage not found!', fg = 'red')
            return


        all_submittable = controller.submittable_nodes()
        _, s2r = manualutils.rule_steps_indices(controller.adageobj)
        nodes_to_submit = [x for x in all_submittable if s2r[x] == rule.identifier]

    if not nodes_to_submit:
        click.secho('No nodes to submit (perhaps already submitted?)', fg = 'yellow')
        return

    controller.submit_nodes(nodes_to_submit)
    click.secho('submitted: {}'.format(nodes_to_submit), fg = 'green')

@mancli.command()
@connection_options
@common_options
def shell(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
         verbosity
         ):
    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    assert ys
    import IPython
    IPython.embed()

@mancli.command()
@connection_options
@common_options
def show(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
         verbosity
         ):

    ctrlarg = controller # for later
    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    controller = ys.controller

    click.secho('''\
Workflow:
---------
state source: {statesource}
successful: {successful}
finished: {finished}
valid: {valid}
# of applicable rules: {applicable}
# of submittable nodes: {submittable}
'''.format(
        statesource = modelsetup if local else 'remote via {}'.format(ctrlarg),
        successful =  click.style(str(controller.successful()), fg = 'green' if controller.successful() else 'red'),
        finished = click.style(str(controller.finished()), fg = 'green' if controller.finished() else 'yellow'),
        valid = click.style(str(controller.validate()), fg = 'green' if controller.validate() else 'red'),
        applicable = len(controller.applicable_rules()),
        submittable = len(controller.submittable_nodes())
    ))
    click_print_applicable_stages(controller)
    click_print_submittable_nodes(controller)


@mancli.command()
@click.argument('name')
@connection_options
@common_options
def preview(name,
            metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
            verbosity
            ):


    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    controller = ys.controller

    new_rules, new_nodes = manualutils.preview_rule(controller.adageobj, name)
    click.secho('Preview of Stage: # new rules: {} # new nodes {}'.format(
        len(new_rules), len(new_nodes)))
    for n in new_nodes:
        click.secho(
            '-> new node "{}" with {} upstream dependencies'.format(n['name'], len(n['parents'])))


def handle_common_options(verbosity):
    logging.basicConfig(level=getattr(logging, verbosity))

def handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local):
    ctrlopts = utils.options_from_eqdelimstring(ctrlopt)
    modelopts = utils.options_from_eqdelimstring(modelopt)
    ys = manualutils.connect(
        metadir = metadir,
        accept_metadir = accept_metadir,
        ctrlstring = controller,
        ctrlopts = ctrlopts,
        modelsetup = modelsetup,
        modelopts = modelopts,
        backendstring = backend if local else None
    )
    return ys


@mancli.command()
@click.option('--track/--no-track', default=True)
@click.option('-n', '--nsteps', default=-1, help = 'number of steps to process. use -1 to for no limit (will run workflow to completion)')
@click.option('-u', '--update-interval', default=1)
@click.option('--interactive/--not-interactive', default=False, help = 'en-/disable user interactio (sign-off graph extensions and packtivity submissions)')
@connection_options
@common_options
def step(track , interactive, nsteps, update_interval,
         metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
         verbosity
         ):

    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)

    execute_steering(
        ys,
        updateinterval = update_interval,
        default_trackers = track,
        interactive = interactive
    )

@mancli.command()
@click.argument('workdir')
@click.argument('workflow')
@click.option('-o', '--offset')
@click.option('-t', '--toplevel', default = os.curdir)
@connection_options
@common_options
def add(offset, toplevel, workdir, workflow,
        metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
        verbosity
        ):

    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)

    workflow_json = workflow_loader.workflow(
        workflow,
        toplevel=toplevel,
        validate=True
    )

    state_provider = LocalFSProvider(LocalFSState([workdir]))
    rules = [JsonStage(json, state_provider) for json in workflow_json['stages']]
    with ys.controller.transaction():
        ys.controller.adageobj.view().addWorkflow(rules)

@mancli.command()
@click.option('-f', '--fileformat', default='pdf')
@click.option('-w', '--workdir', default=os.curdir)
@connection_options
@common_options
def visualize(workdir, fileformat,
              metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
              verbosity
            ):
    from .visualize import write_prov_graph

    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    controller = ys.controller

    write_prov_graph(workdir, controller.adageobj, fileformat)



@mancli.command()
@click.argument('name')
@connection_options
@common_options
def reset(name,
          metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
          verbosity
    ):
    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    controller = ys.controller

    offset, name = name.rsplit('/',1)
    rule = controller.adageobj.view(offset).getRule(name)
    if not rule:
        click.secho('state not found!', fg = 'red')

    r2s, _ = manualutils.rule_steps_indices(controller.adageobj)
    steps_of_rule = r2s[rule.identifier]

    to_reset = steps_of_rule + reset_module.collective_downstream(controller.adageobj, steps_of_rule)
    controller.reset_nodes(to_reset)

if __name__ == '__main__':
    mancli()

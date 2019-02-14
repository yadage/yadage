#!/usr/bin/env python
import functools
import logging
import os

import click
import yaml

import yadage.manualutils as manualutils
import yadage.utils as utils
import yadage.workflow_loader as workflow_loader

from .steering_api import execute_steering
from .steering_object import YadageSteering

log = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)


def connection_options(func):
    @click.option('-m', '--metadir', default='yadagemeta', help = 'directory to store workflow metadata')
    @click.option('--accept-metadir/--no-accept-metadir', default=True)
    @click.option('-r', '--controller', default='frommodel')
    @click.option('-o', '--ctrlopt', multiple=True, default=None, help = 'options for the workflow controller')
    @click.option('-s', '--modelsetup', default='filebacked')
    @click.option('-l', '--modelopt', multiple=True, default=None, help = 'options for the workflow state models')
    @click.option('-b', '--backend', default='foregroundasync')
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
@click.option('-s', '--modelsetup', default='filebacked:yadagemeta/yadage_state.json')
@click.option('-t', '--toplevel', default=os.getcwd())
@click.option('-d', '--dataopt', multiple=True, default=None, help = 'options for the workflow data state')
@click.option('--metadir', default='yadagemeta', help = 'directory to store workflow metadata')
@click.option('--parameter', '-p', multiple=True)
@click.argument('dataarg', default = 'workdir')
@click.argument('workflow', default = 'workflow.yml')
@click.argument('initfiles', nargs = -1)
def init(dataarg, workflow, initfiles, modelsetup, dataopt, metadir, toplevel, parameter):
    if os.path.exists('input.yml') and not initfiles:
        initfiles = ('input.yml',)
    initdata = utils.getinit_data(initfiles, parameter)
    dataopts = utils.options_from_eqdelimstring(dataopt)

    ys = YadageSteering.create(
        dataarg = dataarg,
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
    _, s2r,_ = utils.rule_steps_indices(controller.adageobj)
    for x in controller.adageobj.dag.nodes():
        node = controller.adageobj.dag.getNode(x)
        rule = controller.adageobj.view().getRule(identifier = s2r[node.identifier])
        if node.identifier in controller.submittable_nodes():
            click.secho('node: {} ({}) part of stage {}'.format(node.name, node.identifier,  '/'.join([rule.offset,rule.rule.name])))

def click_print_rule(rule, step_index, subrule_index, step_status):
    steps_of_rule     = step_index[rule.identifier]
    substages_of_rule = subrule_index[rule.identifier]
    rule_stats = {}
    for x in steps_of_rule:
        rule_stats.setdefault(step_status[x],0)
        rule_stats[step_status[x]] += 1
    click.secho('{}/{} [steps: ({})] [subflows: {}]' .format(
        rule.offset, rule.rule.name, '/'.join('{}: {}'.format(k,v) for k,v in rule_stats.items()),
        len(substages_of_rule)
        )
    )

def click_print_applied_stages(controller):
    click.secho('Applied Stages: ', fg='blue')
    r2s, s2r, r2sub = utils.rule_steps_indices(controller.adageobj)
    step_status = {s: str(controller.adageobj.dag.getNode(s).state) for s in s2r.keys()}
    # print(step_status)
    for x in sorted(controller.adageobj.applied_rules,key = lambda r: '{}/{}'.format(r.offset,r.rule.name)):
        click_print_rule(x, r2s, r2sub, step_status)

@mancli.command()
@click.argument('name', default=None, nargs = -1)
@connection_options
@common_options
@click.option('--submit/--no-submit')
def apply_stage(name,
          metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
          verbosity,
          submit
         ):

    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    controller = ys.controller

    if not name:
        click_print_applicable_stages(controller)
        return

    for n in name:
        offset, scopedname = n.rsplit('/',1)
        rule = controller.adageobj.view(offset).getRule(scopedname)
        if not rule:
            click.secho('No such stage {}, pick one of the applicable below:'.format(n), fg='red')
            click_print_applicable_stages(controller)
            return

        if rule.identifier in [r.identifier for r in controller.adageobj.applied_rules]:
            click.secho('Stage {} was already applied.'.format(n), fg = 'yellow')
            continue

        if rule.identifier not in controller.applicable_rules():
            click.secho('Rule {} not yet applicable'.format(n), fg = 'red')
            continue

        controller.apply_rules([rule.identifier])

        if submit:
            _, s2r, _ = utils.rule_steps_indices(controller.adageobj)
            nodes_to_submit = [x for x in controller.submittable_nodes() if s2r[x] == rule.identifier]
            controller.submit_nodes(nodes_to_submit)

        click.secho('Stage {} applied'.format(n), fg = 'green')

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
        _, s2r, _ = utils.rule_steps_indices(controller.adageobj)
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
    click.secho('launching yadage shell', fg = 'green')
    assert ys
    import IPython
    IPython.embed()

@mancli.command()
@connection_options
@common_options
@click.option('--show-processed/--hide-processed', default = False)
def show(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
         verbosity,
         show_processed
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

    if show_processed:
        click_print_applied_stages(controller)



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

    preview = manualutils.preview_rule(controller.adageobj, name)
    if not preview:
        click.secho('cannot preview {}'.format(name))
        raise click.Abort()

    new_rules, new_nodes = preview
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

    if modelsetup == 'filebacked':
        modelsetup = 'filebacked:{}/yadage_state.json'.format(metadir)

    ys = manualutils.connect(
        metadir = metadir,
        accept_metadir = accept_metadir,
        ctrlstring = controller,
        ctrlopts = ctrlopts,
        modelsetup = modelsetup,
        modelopts = modelopts,
        backendstring = backend if local else None
    )
    ys.controller.sync_backend()
    return ys


@mancli.command()
@click.option('--track/--no-track', default=True)
@click.option('-n', '--nsteps', default=-1, help = 'number of steps to process. use -1 to for no limit (will run workflow to completion)')
@click.option('-u', '--update-interval', default=1)
@click.option('-g','--strategy', help = 'set execution stragegy')
@connection_options
@common_options
def step(track , strategy, nsteps, update_interval,
         metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
         verbosity
         ):

    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)

    execute_steering(
        ys,
        updateinterval = update_interval,
        default_trackers = track,
        strategy = strategy
    )

@mancli.command()
@click.argument('dataarg')
@click.argument('workflow')
@click.option('-d', '--dataopts', multiple=True, default=None, help = 'ctrl opts for state provider')
@click.option('-f', '--offset', default = '')
@click.option('-g', '--groupname', default = None)
@click.option('-t', '--toplevel', default = os.curdir)
@click.option('--parameter', '-p', multiple=True)
@connection_options
@common_options
def add(offset, groupname, dataarg, dataopts, workflow, toplevel, parameter,
        metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
        verbosity
        ):
    parameter = utils.options_from_eqdelimstring(parameter)
    dataopts = utils.options_from_eqdelimstring(dataopts)
    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    controller = ys.controller

    stages = []
    if parameter:
        inspec = utils.init_stage_spec(parameter, False, [], 'init')
        stages.append(inspec)

    stages = stages + workflow_loader.workflow(
        workflow,
        toplevel=toplevel,
        validate=True
    )['stages']
    controller.add_rules(stages, dataarg, offset = offset, groupname = groupname, dataopts = dataopts)

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
    if controller.backend:
        controller.sync_backend()


    write_prov_graph(workdir, controller.adageobj, vizformat=fileformat)

@mancli.command()
@click.argument('name', default=None)
@click.option('-p','--patchspec', default=None)
@connection_options
@common_options
def edit_stage(name,patchspec,
          metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
          verbosity
    ):
    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    controller = ys.controller

    if not name:
        click.secho('No stage specified. Pick one of the stages below:  ', fg = 'red')
        click_print_applied_stages(controller)
        return

    offset, scopedname = name.rsplit('/',1)
    rule = controller.adageobj.view(offset).getRule(scopedname)

    s = yaml.safe_dump(rule.rule.stagespec, default_flow_style = False)

    if patchspec:
        edited = yaml.load(open(patchspec))
    else:
        edited = yaml.load(click.edit(s, editor='vi'))
    controller.patch_rule(rule.identifier, edited)
    click.secho('updated {}'.format(name), fg = 'green')

@mancli.command()
@click.argument('name', default=None, nargs = -1)
@connection_options
@common_options
def undo_stage(name,
          metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
          verbosity
    ):
    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    controller = ys.controller

    if not name:
        click.secho('No stage specified. Pick one of the stages below:  ', fg = 'red')
        click_print_applied_stages(controller)
        return

    for n in name:
        offset, scopedname = n.rsplit('/',1)
        rule = controller.adageobj.view(offset).getRule(scopedname)
        controller.undo_rules([rule.identifier])
        click.secho('undone {}'.format(n), fg = 'green')

@mancli.command()
@click.argument('name', default=None)
@connection_options
@common_options
def remove_stage(name,
          metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local,
          verbosity
    ):
    handle_common_options(verbosity)
    ys = handle_connection_options(metadir, accept_metadir, controller, ctrlopt, modelsetup, modelopt, backend, local)
    controller = ys.controller

    if not name:
        click.secho('No stage specified. Pick one of the stages below:  ', fg = 'red')
        click_print_applied_stages(controller)
        return

    offset, name = name.rsplit('/',1)
    rule = controller.adageobj.view(offset).getRule(name)
    controller.remove_rules([rule.identifier])

@mancli.command()
@click.argument('name')
@connection_options
@common_options
def reset_stage(name,
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

    r2s, _, _ = utils.rule_steps_indices(controller.adageobj)
    steps_of_rule = r2s[rule.identifier]
    controller.reset_nodes(steps_of_rule)

if __name__ == '__main__':
    mancli()

#!/usr/bin/env python
import logging
import click
import os
import manualutils
import clihelpers
from steering_object import YadageSteering
from visualize import write_prov_graph
from controllers import create_model_fromstring, PersistentController
from packtivity.statecontexts.posixfs_context import LocalFSProvider,LocalFSState
from stages import jsonStage
import interactive
import reset as reset_module
import workflow_loader

log = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

@click.group()
def mancli():
    pass

@mancli.command()
@click.option('-s', '--statectrl', default='filebacked:yadage_state.json')
@click.option('-t', '--toplevel', default=os.getcwd())
@click.option('-a', '--inputarchive', default=None)
@click.option('-d','--initdir', default='init', help = "relative path (to workdir) to initialiation data directory")
@click.option('--parameter', '-p', multiple=True)
@click.argument('workdir')
@click.argument('workflow')
@click.argument('initfiles', nargs = -1)
def init(workdir, workflow, initfiles, statectrl, initdir, toplevel, parameter, inputarchive):
    initdata = clihelpers.getinit_data(initfiles, parameter)

    if inputarchive:
        initdir = clihelpers.prepare_workdir_from_archive(workdir, inputarchive)
    else:
        initdir = os.path.join(workdir,initdir)

    ys = YadageSteering()
    ys.prepare_workdir(workdir)
    ys.init_workflow(workflow, toplevel, initdata, ctrlsetup = statectrl, initdir = initdir)


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
            click.secho('node: {}({}) part of stage {}'.format(node.name, node.identifier,  '/'.join([rule.offset,rule.rule.name])))

@mancli.command()
@click.option('-n','--name', default=None)
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
@click.option('-v', '--verbosity', default='ERROR')
def apply(name, statetype, verbosity):
    logging.basicConfig(level=getattr(logging, verbosity))

    model      = create_model_fromstring(statetype)
    controller = PersistentController(model)

    if not name:
        click_print_applicable_stages(controller)
        return


    offset, name = name.split('/')
    rule = controller.adageobj.view(offset).getRule(name)
    if not rule:
        click.secho('No such stage, pick one of the applicable below:', fg='red')
        click_print_applicable_stages(controller)
        return

    if rule in controller.adageobj.applied_rules:
        click.secho('Already applied.', fg = 'yellow')
        return

    controller.apply_rules([rule.identifier])
    click.secho('stage applied', fg = 'green')

@mancli.command()
@click.option('-n','--nodeid', default=None)
@click.option('-a','--allof', default=None)
@click.option('-o', '--offset', default='')
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
@click.option('-v', '--verbosity', default='ERROR')
def submit(nodeid, allof, offset, statetype, verbosity):
    logging.basicConfig(level=getattr(logging, verbosity))

    model   = create_model_fromstring(statetype)
    controller = PersistentController(model)
    controller.backend = clihelpers.setupbackend_fromstring('celery')


    if not (allof or nodeid):
        click_print_submittable_nodes(controller)
        return

    if nodeid:
        nodes_to_submit = [nodeid] if nodeid in controller.submittable_nodes() else []
    if allof:
        offset, name = allof.split('/')
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
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
def show(statetype):
    model      = create_model_fromstring(statetype)
    controller = PersistentController(model)
    controller.backend = clihelpers.setupbackend_fromstring('celery')
    click.secho('''
Workflow:
---------
state source: {statetype}
successful: {successful}
finished: {finished}
valid: {valid}
# of applicable rules: {applicable}
# of submittable nodes: {submittable}
'''.format(
    statetype = statetype,
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
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
def preview(name,statetype):
    model      = create_model_fromstring(statetype)
    controller = PersistentController(model)
    controller.backend = clihelpers.setupbackend_fromstring('celery')

    new_rules, new_nodes = manualutils.preview_rule(controller.adageobj, name)
    click.secho('Preview of Stage: # new rules: {} # new nodes {}'.format(
        len(new_rules), len(new_nodes)))
    for n in new_nodes:
        click.secho(
            '-> new node "{}" with {} upstream dependencies'.format(n['name'], len(n['parents'])))
@mancli.command()
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
@click.option('-v', '--verbosity', default='ERROR')
@click.option('-n', '--nsteps', default=-1, help = 'number of steps to process. use -1 to for no limit (will run workflow to completion)')
@click.option('-u', '--update-interval', default=1)
def step(statetype, verbosity, nsteps, update_interval):
    logging.basicConfig(level=getattr(logging, verbosity))

    maxsteps = nsteps if nsteps >= 0 else None
    model   = create_model_fromstring(statetype)
    backend = clihelpers.setupbackend_fromstring('celery')

    extend, submit = interactive.interactive_deciders(idbased = True)
    ys = YadageSteering()
    ys.adage_argument()
    ys.controller = PersistentController(model)
    ys.run_adage(backend,
        maxsteps = maxsteps,
        default_trackers = False,
        submit_decider = submit,
        extend_decider = extend,
        update_interval = update_interval
    )

@mancli.command()
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
@click.option('-v', '--verbosity', default='ERROR')
@click.option('-o', '--offset')
@click.option('-t', '--toplevel', default = os.curdir)
@click.argument('workdir')
@click.argument('workflow')
def add(statetype, verbosity, offset, toplevel, workdir, workflow):
    logging.basicConfig(level=getattr(logging, verbosity))

    model      = create_model_fromstring(statetype)
    controller = PersistentController(model)

    workflow_json = workflow_loader.workflow(
        workflow,
        toplevel=toplevel,
        validate=True
    )

    state_provider = LocalFSProvider(LocalFSState([workdir]))
    rules = [jsonStage(json, state_provider) for json in workflow_json['stages']]
    with controller.transaction():
        controller.adageobj.view().addWorkflow(rules)

@mancli.command()
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
@click.option('-f', '--fileformat', default='pdf')
@click.option('-w', '--workdir', default=os.curdir)
def visualize(statetype, workdir, fileformat):

    model      = create_model_fromstring(statetype)
    controller = PersistentController(model)

    write_prov_graph(workdir, controller.adageobj, fileformat)



@mancli.command()
@click.argument('name')
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
def reset(statetype, name):
    model   = create_model_fromstring(statetype)
    controller = PersistentController(model)

    offset, name = name.split('/')
    rule = controller.adageobj.view(offset).getRule(name)
    if not rule:
        click.secho('state not found!', fg = 'red')

    r2s, _ = manualutils.rule_steps_indices(controller.adageobj)
    steps_of_rule = r2s[rule.identifier]

    to_reset = steps_of_rule + reset_module.collective_downstream(controller.adageobj, steps_of_rule)

    controller.reset_nodes(to_reset)

if __name__ == '__main__':
    mancli()

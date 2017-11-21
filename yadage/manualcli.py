#!/usr/bin/env python
import logging
import click
import os
from packtivity.statecontexts.posixfs_context import LocalFSState
from yadage.state_providers.localposix import LocalFSProvider

from .steering_object import YadageSteering
from .controllers import PersistentController
from .wflowstate import load_model_fromstring
from .stages import JsonStage
import yadage.manualutils as manualutils
import yadage.utils as utils
import yadage.interactive as interactive
import yadage.reset as reset_module
import yadage.workflow_loader as workflow_loader

log = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)

@click.group()
def mancli():
    pass

@mancli.command()
@click.option('-s', '--statesetup', default='filebacked:yadage_state.json')
@click.option('-t', '--toplevel', default=os.getcwd())
@click.option('-d', '--dataopt', multiple=True, default=None, help = 'options for the workflow data state')
@click.option('--metadir', default=None, help = 'directory to store workflow metadata')
@click.option('--parameter', '-p', multiple=True)
@click.argument('workdir')
@click.argument('workflow')
@click.argument('initfiles', nargs = -1)
def init(workdir, workflow, initfiles, statesetup, dataopt, metadir, toplevel, parameter):
    initdata = utils.getinit_data(initfiles, parameter)
    dataopts = utils.options_from_eqdelimstring(dataopt)

    ys = YadageSteering()
    ys.prepare(workdir,
        metadir = metadir,
        dataopts = dataopts
    )
    ys.init_workflow(workflow, initdata = initdata, toplevel = toplevel, modelsetup = statesetup)


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
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
@click.option('-l', '--modelopt', multiple=True, default=None, help = 'options for the workflow state models')
@click.option('-v', '--verbosity', default='ERROR')
@click.option('-b', '--backend', default='celery')
def apply(name, statetype, verbosity, backend,modelopt):
    logging.basicConfig(level=getattr(logging, verbosity))

    stateopts = utils.options_from_eqdelimstring(modelopt)
    model   = load_model_fromstring(statetype,stateopts)
    backend =  utils.setupbackend_fromstring(backend)
    controller = PersistentController(model,backend)

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
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
@click.option('-l', '--modelopt', multiple=True, default=None, help = 'options for the workflow state models')
@click.option('-v', '--verbosity', default='ERROR')
@click.option('-b', '--backend', default='celery')
def submit(nodeid, allof, offset, statetype, modelopt, verbosity, backend):
    logging.basicConfig(level=getattr(logging, verbosity))

    stateopts = utils.options_from_eqdelimstring(modelopt)
    model   = load_model_fromstring(statetype,stateopts)
    backend =  utils.setupbackend_fromstring(backend)
    controller = PersistentController(model,backend)

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
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
@click.option('-l', '--modelopt', multiple=True, default=None, help = 'options for the workflow state models')
@click.option('-b', '--backend', default='celery')
def show(statetype, backend, modelopt):
    stateopts = utils.options_from_eqdelimstring(modelopt)
    model   = load_model_fromstring(statetype,stateopts)
    controller = PersistentController(model)
    controller.backend = utils.setupbackend_fromstring(backend)

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
@click.option('-l', '--modelopt', multiple=True, default=None, help = 'options for the workflow state models')
@click.option('-b', '--backend', default='celery')
def preview(name,statetype,backend,modelopt):
    stateopts = utils.options_from_eqdelimstring(modelopt)
    model   = load_model_fromstring(statetype,stateopts)
    controller = PersistentController(model)
    controller.backend = utils.setupbackend_fromstring(backend)

    new_rules, new_nodes = manualutils.preview_rule(controller.adageobj, name)
    click.secho('Preview of Stage: # new rules: {} # new nodes {}'.format(
        len(new_rules), len(new_nodes)))
    for n in new_nodes:
        click.secho(
            '-> new node "{}" with {} upstream dependencies'.format(n['name'], len(n['parents'])))
@mancli.command()
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
@click.option('-l', '--modelopt', multiple=True, default=None, help = 'options for the workflow state models')
@click.option('-v', '--verbosity', default='ERROR')
@click.option('-n', '--nsteps', default=-1, help = 'number of steps to process. use -1 to for no limit (will run workflow to completion)')
@click.option('-u', '--update-interval', default=1)
@click.option('-b', '--backend', default='celery')
def step(statetype, verbosity, nsteps, update_interval,backend,modelopt):
    logging.basicConfig(level=getattr(logging, verbosity))

    maxsteps = nsteps if nsteps >= 0 else None
    stateopts = utils.options_from_eqdelimstring(modelopt)
    model   = load_model_fromstring(statetype,stateopts)
    backend = utils.setupbackend_fromstring(backend)

    extend, submit = interactive.interactive_deciders(idbased = True)
    ys = YadageSteering()
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
@click.option('-l', '--modelopt', multiple=True, default=None, help = 'options for the workflow state models')
@click.option('-v', '--verbosity', default='ERROR')
@click.option('-o', '--offset')
@click.option('-t', '--toplevel', default = os.curdir)
@click.argument('workdir')
@click.argument('workflow')
def add(statetype, verbosity, offset, toplevel, workdir, workflow,modelopt):
    logging.basicConfig(level=getattr(logging, verbosity))

    stateopts = utils.options_from_eqdelimstring(modelopt)
    model   = load_model_fromstring(statetype,stateopts)
    controller = PersistentController(model)

    workflow_json = workflow_loader.workflow(
        workflow,
        toplevel=toplevel,
        validate=True
    )

    state_provider = LocalFSProvider(LocalFSState([workdir]))
    rules = [JsonStage(json, state_provider) for json in workflow_json['stages']]
    with controller.transaction():
        controller.adageobj.view().addWorkflow(rules)

@mancli.command()
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
@click.option('-l', '--modelopt', multiple=True, default=None, help = 'options for the workflow state models')
@click.option('-f', '--fileformat', default='pdf')
@click.option('-w', '--workdir', default=os.curdir)
def visualize(statetype, workdir, fileformat,modelopt):
    from .visualize import write_prov_graph

    stateopts = utils.options_from_eqdelimstring(modelopt)
    model   = load_model_fromstring(statetype,stateopts)
    controller = PersistentController(model)

    write_prov_graph(workdir, controller.adageobj, fileformat)



@mancli.command()
@click.argument('name')
@click.option('-s', '--statetype', default='filebacked:yadage_state.json')
@click.option('-l', '--modelopt', multiple=True, default=None, help = 'options for the workflow state models')
def reset(statetype, name,modelopt):
    stateopts = utils.options_from_eqdelimstring(modelopt)
    model   = load_model_fromstring(statetype,stateopts)
    controller = PersistentController(model)

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

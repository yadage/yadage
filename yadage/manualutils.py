import json
import click
import time
import copy
import yadage.workflow_loader
import yadage.yadagemodels
import yadage.visualize
import yadage.interactive
import serialize
import adage.visualize as av

from contextlib import contextmanager


@contextmanager
def workflowctx(workdir, statefile, backendfile):
    statefile = '{}/{}'.format(get_yadagedir(workdir), statefile)
    backendfile = '{}/{}'.format(get_yadagedir(workdir), backendfile)
    backend, workflow = load_state(statefile)

    yield backend, workflow

    serialize.snapshot(
        workflow,
        statefile,
        backendfile
    )
    av.save_dot(
        av.colorize_graph_at_time(workflow.dag, time.time()).to_string(),
        '{}/{}'.format(get_yadagedir(workdir),
                       'adage.png'),
        'png'
    )


def decide_rule(rule, state):
    click.secho('we could extend DAG with rule', fg='blue')
    click.secho('rule: {}/{} ({})'.format(rule.offset,
                                          rule.rule.name, rule.identifier))
    shall = raw_input(click.style(
        "Shall we? (y/N) ", fg='blue')).lower() == 'y'
    if shall:
        click.secho('ok we will extend.', fg='green')
    else:
        click.secho('maybe another time...', fg='yellow')
    return shall


def decide_step(dag, nodeobj):
    print 'we could submit a DAG node (id: {}) DAG is: {}'.format(nodeobj, dag)
    shall = raw_input(click.style("Shall we? (y/N) ",
                                  fg='magenta')).lower() == 'y'
    if shall:
        click.secho('ok we will submit.', fg='green')
    else:
        click.secho('will not submit for now...', fg='yellow')
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


class VariableProxy():
    @staticmethod
    def fromJSON(data):
        import packtivity.asyncbackends
        import yadage.backends.packtivitybackend
        if data['proxyname']=='InitProxy':
            return yadage.backends.packtivitybackend.InitProxy.fromJSON(data)
        elif data['proxyname']=='CeleryProxy':
            return packtivity.asyncbackends.CeleryProxy.fromJSON(data)
        else:
            raise RuntimeError('only celery support for now...')

def load_state(statefile):
    from clihelpers import setupbackend_fromstring

    backend = setupbackend_fromstring('celery')

    click.secho('loading state from {}'.format(statefile))
    workflow = yadage.yadagemodels.YadageWorkflow.fromJSON(
        json.load(open(statefile)),
        VariableProxy,
        backend
    )
    return backend, workflow

def finalize_manual(workdir, workflow):
    click.secho('workflow done.', fg='green')
    yadage.visualize.write_prov_graph(get_yadagedir(workdir), workflow)


def applicable_rules(workflow, justids=False):
    return [(x.identifier if justids else x) for x in workflow.rules if x.applicable(workflow)]


def preview_rule(wflow, name=None, offset='', identifier=None):
    newflow = yadage.yadagemodels.YadageWorkflow.fromJSON(
        copy.deepcopy(wflow.json())
    )

    if identifier:
        rule = newflow.view().getRule(identifier=identifier)
    else:
        rule = newflow.view().getRule(name, offset)

    if not rule.applicable(newflow):
        return

    rule.apply(newflow)
    newflow.rules.remove(rule)
    newflow.applied_rules.append(rule)

    existing_rules = [x.identifier for x in (
        wflow.rules + wflow.applied_rules)]
    existing_nodes = wflow.dag.nodes()

    new_rules = [{'name': x.rule.name, 'offset': x.offset}
                 for x in newflow.rules if x.identifier not in existing_rules]
    new_nodes = [{'name': newflow.dag.getNode(n).name, 'parents': newflow.dag.predecessors(
        n)} for n in newflow.dag.nodes() if n not in existing_nodes]
    return new_rules, new_nodes

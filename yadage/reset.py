import logging

import networkx as nx

import yadage.utils as utils

log = logging.getLogger(__name__)

def reset_node_state(node):
    node.submit_time = None
    node.ready_by_time = None
    node.resultproxy = None
    node.backend = None

def reset_step(workflow, step):
    log.debug('resetting %s', step)
    s = workflow.dag.getNode(step)
    reset_node_state(s)
    try:
        s.task.state.reset()
    except AttributeError:
        pass

def remove_rule(workflow, ruleid):
    assert ruleid in [x.identifier for x in workflow.rules]
    r = workflow.view().getRule(identifier = ruleid)
    workflow.rules.remove(r)
    workflow.view(r.offset).bookkeeper['_meta']['stages'].remove(ruleid)

def remove_rules(workflow, ruleids):
    for r in ruleids:
        remove_rule(workflow, r)

def undo_rule(workflow, ruleid):
    r2s, s2r   = utils.rule_steps_indices(workflow)

    if not ruleid in [r.identifier for r in workflow.applied_rules]:
        log.info('rule not in list of applied rules. possibly already undone during recursion.')
        return

    down_rules = list(set([s2r[s] for s in collective_downstream(workflow, r2s[ruleid])]))

    assert ruleid not in down_rules
    #TODO: not an issue now, but if stages schedule linked nodes we might
    #we might see downstream steps that are part of the same rule

    if not down_rules:
        r = workflow.view().getRule(identifier = ruleid)
        log.info('undoing %s', r)
        idx   = workflow.applied_rules.index(r)
        stepids = r2s[ruleid]
        steps = [workflow.dag.getNode(nid) for nid in stepids]

        # reset states of the step
        reset_steps(workflow,stepids)
        #
        # delete the steps
        for n in steps:
            workflow.dag.removeNode(n)

        #remove rule from applied list
        r = workflow.applied_rules.pop(idx)

        #reset all index data for stage
        workflow.view(r.offset).steps.pop(r.rule.name)

        #remove steps and stage from bookkeeping
        for s in stepids:
            workflow.view(r.offset).bookkeeper['_meta']['steps'].remove(s)

        workflow.view(r.offset).bookkeeper['_meta']['stages'].remove(ruleid)

        #re-append the rule
        workflow.view(r.offset).addRule(r.rule)
        return

    while True:
        if not down_rules:
            break
        for r in down_rules:
            undo_rule(workflow,r)
        r2s, s2r   = utils.rule_steps_indices(workflow)
        down_rules = list(set([s2r[s] for s in collective_downstream(workflow, r2s[ruleid])]))
    undo_rule(workflow,ruleid)

def undo_rules(workflow, ruleids):
    log.debug('undo rules')
    for r in ruleids:
        undo_rule(workflow, r)

def reset_steps(workflow, steps):
    for s in steps:
        reset_step(workflow, s)

def collective_downstream(workflow, steps):
    downstream = set()
    for step in steps:
        for x in nx.descendants(workflow.dag, step):
            downstream.add(x)
    return list(downstream)

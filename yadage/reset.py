import logging

import networkx as nx

import yadage.utils as utils
import adage.nodestate
log = logging.getLogger(__name__)

def reset_node_state(node):
    node.submit_time = None
    node.ready_by_time = None
    node.resultproxy = None
    node.backend = None
    node.expected_result = None
    node._result = None
    node._state = adage.nodestate.DEFINED

def reset_step(workflow, step):
    s = workflow.dag.getNode(step)
    log.debug('resetting DAG node %s/%s:%s',s.task.metadata['wflow_offset'],s.task.metadata['wflow_stage'],s.task.metadata['wflow_stage_node_idx'])
    reset_node_state(s)
    try:
        s.task.state.reset()
    except AttributeError:
        pass

def remove_rule(workflow, ruleid):
    assert ruleid in [x.identifier for x in workflow.rules]
    r = workflow.view().getRule(identifier = ruleid)
    log.debug('>>> removing rule %s/%s',r.offset,r.rule.name)
    workflow.rules.remove(r)
    view = workflow.view(r.offset)
    stagemeta = view.bookkeeper['_meta']['stages']
    view.bookkeeper['_meta']['stages'] = [x for x in stagemeta if not x==ruleid]

def remove_rules(workflow, ruleids):
    for r in ruleids:
        remove_rule(workflow, r)

def undo_rule(workflow, ruleid):
    r2s, s2r, r2subscopes  = utils.rule_steps_indices(workflow)

    if not ruleid in [r.identifier for r in workflow.applied_rules]:
        ruleobj = workflow.view().getRule(identifier = ruleid)
        log.debug('rule %s/%s not in list of applied rules. possibly already undone during recursion.',ruleobj.offset,ruleobj.rule.name)
        return

    downstream_nodes_rules = list(set([s2r[s] for s in collective_downstream(workflow, r2s[ruleid])]))


    r = workflow.view().getRule(identifier = ruleid)
    log.debug('undoing %s/%s. downstream_nodes_rules: %s', r.offset, r.rule.name, len(downstream_nodes_rules))

    assert ruleid not in downstream_nodes_rules
    #TODO: not an issue now, but if stages schedule linked nodes we might
    #we might see downstream steps that are part of the same rule

    if not downstream_nodes_rules:
        idx   = workflow.applied_rules.index(r)
        stepids = r2s[ruleid]
        steps = [workflow.dag.getNode(nid) for nid in stepids]

        # reset states of the step
        reset_steps(workflow,stepids)
        #
        # delete the steps
        for n in steps:
            log.debug('remove DAG node %s/%s:%s',n.task.metadata['wflow_offset'],n.task.metadata['wflow_stage'],n.task.metadata['wflow_stage_node_idx'])
            workflow.dag.removeNode(n)

        for subscope in r2subscopes[ruleid]:
            subrules = utils.stages_in_scope(workflow,subscope)
            log.debug('remove subscope %s with %s subrules', subscope, len(subrules))
            for subruleidx,subrule in enumerate(subrules):
                subruleobj = workflow.view().getRule(identifier = subrule)
                log.debug('undo sub DAG rule %s/%s', subruleobj.offset, subruleobj.rule.name)
                undo_rule(workflow,subrule)

            log.debug('removing %s subrules', len(subrules))
            for subruleidx,subrule in enumerate(subrules):
                subruleobj = workflow.view().getRule(identifier = subrule)
                log.debug('remove sub DAG rule %s/%s', subruleobj.offset, subruleobj.rule.name)
                remove_rule(workflow,subrule)
        #remove rule from applied list
        r = workflow.applied_rules.pop(idx)

        #reset all index data for stage
        workflow.view(r.offset).steps.pop(r.rule.name)

        #remove steps and stage from bookkeeping
        for s in stepids:
            workflow.view(r.offset).bookkeeper['_meta']['steps'].remove(s)

        workflow.view(r.offset).bookkeeper['_meta']['stages'].remove(ruleid)

        #re-append the rule
        log.debug('re-appened {}/{}'.format(r.offset,r.rule.name))

        # workflow.steps[stage][-1]['_offset'] = offset
        newid = workflow.view(r.offset).addRule(r.rule, identifier = r.identifier)
        assert newid == r.identifier

        log.debug('undo any rules that would not be applicable now')
        for rule in workflow.applied_rules:
            if not rule.applicable(workflow):
                ruleobj = workflow.view().getRule(identifier = rule.identifier)
                log.debug('rule would not be appilcable in current state so undo >> %s/%s', ruleobj.offset, ruleobj.rule.name)
                undo_rule(workflow,rule.identifier)

        return

    log.debug('to undo a rule, we need to undo rules responsible for any of the downstream nodes of the rules of this stage')
    while True:
        if not downstream_nodes_rules:
            break
        for r in downstream_nodes_rules:
            log.debug('undoing a downstream rule')
            undo_rule(workflow,r)
            log.debug('undone a downstream rule')
        r2s, s2r, _   = utils.rule_steps_indices(workflow)
        downstream_nodes_rules = list(set([s2r[s] for s in collective_downstream(workflow, r2s[ruleid])]))
        log.debug('re-asses if there are still any downstream rules: {}'.format(len(downstream_nodes_rules)))
    log.debug('undo rule (now without any downstream rules)')
    undo_rule(workflow,ruleid)



def undo_rules(workflow, ruleids):
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

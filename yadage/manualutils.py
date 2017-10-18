import jsonpointer
from .wflowstate import make_deserializer

def rule_steps_indices(workflow):
    rule_to_steps_index = {}
    steps_to_rule_index = {}
    for rule in workflow.rules + workflow.applied_rules:
        path = '/'.join([rule.offset, rule.rule.name])
        p = jsonpointer.JsonPointer(path)
        try:
            steps_of_rule = [x['_nodeid'] for x in p.resolve(workflow.stepsbystage) if '_nodeid' in x]
        except jsonpointer.JsonPointerException:
            steps_of_rule = []
        rule_to_steps_index[rule.identifier] = steps_of_rule
        for step in steps_of_rule:
            steps_to_rule_index[step] = rule.identifier
    return rule_to_steps_index, steps_to_rule_index

def preview_rule(wflow, name = None, identifier=None):
    stateopts = {}
    wflowmaker = make_deserializer(stateopts)
    newflow = wflowmaker(wflow.json())

    if identifier:
        rule = newflow.view().getRule(identifier=identifier)
    else:
        offset, name = name.split('/')
        rule = newflow.view(offset).getRule(name)

    if not rule.applicable(newflow):
        return

    rule.apply(newflow)
    newflow.rules.remove(rule)
    newflow.applied_rules.append(rule)

    existing_rules = [x.identifier for x in (wflow.rules + wflow.applied_rules)]
    existing_nodes = wflow.dag.nodes()

    new_rules = [{'name': x.rule.name, 'offset': x.offset} for x in newflow.rules if x.identifier not in existing_rules]
    new_nodes = [{'name': newflow.dag.getNode(n).name, 'parents': newflow.dag.predecessors(n)} for n in newflow.dag.nodes() if n not in existing_nodes]
    return new_rules, new_nodes

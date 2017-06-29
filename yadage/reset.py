import networkx as nx

def reset_node_state(node):
    node.submit_time = None
    node.ready_by_time = None
    node.resultproxy = None
    node.backend = None

def reset_step(workflow, step):
    s = workflow.dag.getNode(step)
    reset_node_state(s)
    try:
        s.task.context.reset()
    except AttributeError:
        pass

def reset_steps(workflow, steps):
    for s in steps:
        reset_step(workflow, s)

def collective_downstream(workflow, steps):
    downstream = set()
    for step in steps:
        for x in nx.descendants(workflow.dag, step):
            downstream.add(x)
    return list(downstream)


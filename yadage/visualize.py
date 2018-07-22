import logging
import shlex
import subprocess

import jsonpointer
import pydotplus

log = logging.getLogger(__name__)


def fillscope(cluster, workflow, scope='', subcluster=True):
    # scopecluster = stagecluster = pydotplus.graphviz.Cluster(graph_name =
    # '_'.join(stagescopeprts),

    if subcluster:
        scopecluster = pydotplus.graphviz.Cluster(
            graph_name=scope.replace('/', ''),
            label=''.join(['[{}]'.format(p) for p in scope.split('/')[1:]]),
            style='solid',
            color='blue')
        cluster.add_subgraph(scopecluster)
    else:
        scopecluster = cluster
    scopeptr = jsonpointer.JsonPointer(scope)
    scoped = scopeptr.resolve(workflow.stepsbystage)
    for stage, elements in scoped.items():
        stagescopeprts = scopeptr.parts + [stage]

        if subcluster:
            stagecluster = pydotplus.graphviz.Cluster(
                graph_name='_'.join(stagescopeprts),
                label=stage,
                labeljust='l',
                color='grey',
                style='dashed')
            scopecluster.add_subgraph(stagecluster)
        else:
            stagecluster = scopecluster

        nodes_to_connect = []
        for i, element in enumerate(elements):
            if '_nodeid' in element:
                nodes_to_connect.append(element['_nodeid'])
                element = element['_nodeid']
                targetcl = stagecluster if stage != 'init' else scopecluster
                shape = 'diamond' if stage in ['init','output'] else 'box'
                label = '' if stage in ['init','output'] else '{}[{}]'.format(stage, i)
                additional = {'fixedsize': True, 'height': 0.2,
                              'width': 0.2} if stage in ['init','output'] else {}
                targetcl.add_node(pydotplus.graphviz.Node(
                    element, label=label, color='blue', shape=shape, **additional))
                result = workflow.dag.getNode(element).result
                if result:
                    add_result(targetcl, element,result)
            elif type(element) == dict:
                # recurse...
                fillscope(
                    stagecluster, workflow, jsonpointer.JsonPointer.from_parts(
                        scopeptr.parts + [stage, i]).path,
                    subcluster=subcluster
                )
        connect_nodes(cluster,workflow,nodes_to_connect)


def path_to_id(stepid, path):
    return '{}_{}'.format(stepid, path.replace('/', '_'))

def add_result(graph, parent, typedleafs):
    leafpointers = [p for p,v in typedleafs.leafs()]

    for leaf in leafpointers:
        leafid = path_to_id(parent, leaf.path)
        # value = leaf.resolve(jsondata)
        source = ''.join(['[{}]'.format(p) for p in leaf.parts])
        label = '{}'.format(source)
        graph.add_node(pydotplus.graphviz.Node(
            leafid, label=label, color='red'))
        graph.add_edge(pydotplus.graphviz.Edge(parent, leafid))


def attach_to_results(provgraph, workflow, node):
    nodeobj = workflow.dag.getNode(node)
    for dep in nodeobj.task.inputs:
        resultid = path_to_id(dep.stepid, dep.pointer.path)
        provgraph.add_edge(pydotplus.graphviz.Edge(resultid, node))


def connect_nodes(provgraph, workflow, nodes):
    for node in nodes:
        attach_to_results(provgraph, workflow, node)


def provdotgraph(workflow, subcluster=True, scope = ''):
    provgraph = pydotplus.graphviz.Graph()
    fillscope(provgraph, workflow, scope = scope, subcluster=subcluster)
    return provgraph


def write_prov_graph(workdir, workflow, scope = '', vizformat='pdf'):
    provgraph = provdotgraph(workflow, subcluster=True, scope = scope)

    with open('{}/yadage_workflow_instance.dot'.format(workdir), 'w') as dotfile:
        dotfile.write(provgraph.to_string())

    with open('{}/yadage_workflow_instance.{}'.format(workdir, vizformat), 'w') as outfile:
        subprocess.call(shlex.split(
            'dot -T{} {}/yadage_workflow_instance.dot'.format(vizformat, workdir)), stdout=outfile)

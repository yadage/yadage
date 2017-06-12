import shlex
import pydotplus
import jsonpointer
import logging
import jq
import subprocess

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
    for stage, elements in scoped.iteritems():
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

        for i, element in enumerate(elements):
            if '_nodeid' in element:
                element = element['_nodeid']
                targetcl = stagecluster if stage != 'init' else scopecluster
                shape = 'diamond' if stage == 'init' else 'box'
                label = '' if stage == 'init' else '{}[{}]'.format(stage, i)
                additional = {'fixedsize': True, 'height': 0.2,
                              'width': 0.2} if stage == 'init' else {}
                targetcl.add_node(pydotplus.graphviz.Node(
                    element, label=label, color='blue', shape=shape, **additional))
                add_result(targetcl, element,
                           workflow.dag.getNode(element).result)
            elif type(element) == dict:
                # recurse...
                fillscope(
                    stagecluster, workflow, jsonpointer.JsonPointer.from_parts(
                        scopeptr.parts + [stage, i]).path,
                    subcluster=subcluster
                )


def path_to_id(stepid, path):
    return '{}_{}'.format(stepid, path.replace('/', '_'))

def add_result(graph, parent, jsondata):
    allleafs = jq.jq('leaf_paths').transform(jsondata, multiple_output=True)
    leafpointers = [jsonpointer.JsonPointer.from_parts(x) for x in allleafs]

    for leaf in leafpointers:
        leafid = path_to_id(parent, leaf.path)
        # value = leaf.resolve(jsondata)
        source = ''.join(['[{}]'.format(p) for p in leaf.parts])
        label = '{}'.format(source)
        graph.add_node(pydotplus.graphviz.Node(
            leafid, label=label, color='red'))
        graph.add_edge(pydotplus.graphviz.Edge(parent, leafid))


def attach_to_results(provgraph, workflow, node):
    for dep in workflow.dag.getNode(node).task.inputs:
        resultid = path_to_id(dep.stepid, dep.pointer.path)
        provgraph.add_edge(pydotplus.graphviz.Edge(resultid, node))


def connect(provgraph, workflow):
    for node in workflow.dag.nodes():
        attach_to_results(provgraph, workflow, node)


def provdotgraph(workflow, subcluster=True):
    provgraph = pydotplus.graphviz.Graph()
    fillscope(provgraph, workflow, subcluster=subcluster)
    connect(provgraph, workflow)
    return provgraph


def write_prov_graph(workdir, workflow, vizformat='pdf'):
    provgraph = provdotgraph(workflow, subcluster=True)

    with open('{}/yadage_workflow_instance.dot'.format(workdir), 'w') as dotfile:
        dotfile.write(provgraph.to_string())

    with open('{}/yadage_workflow_instance.{}'.format(workdir, vizformat), 'w') as outfile:
        subprocess.call(shlex.split(
            'dot -T{} {}/yadage_workflow_instance.dot'.format(vizformat, workdir)), stdout=outfile)

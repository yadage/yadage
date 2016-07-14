import shlex
import pydotplus
import jsonpointer
import networkx as nx
from networkx.drawing.nx_pydot import write_dot
import logging
import jq
import subprocess

log = logging.getLogger(__name__)

def simple_stage_graph(workflow):
    graph = nx.DiGraph()
    for stage in workflow.stages.values():
        graph.add_node(stage.name)
        for x in stage.dependencies:
            graph.add_edge(x,stage.name)
    return graph

def write_stage_graph(workdir,workflow):
    graph = simple_stage_graph(workflow)
    write_dot(graph,'{}/yadage_stages.dot'.format(workdir))
    subprocess.call(shlex.split('dot -Tpdf {}/yadage_stages.dot'.format(workdir)),
                    stdout = open('{}/yadage_stages.pdf'.format(workdir),'w'))

def output_id(stepid,outputkey,index):
    identifier = 'output_{}_{}'.format(stepid,outputkey)
    if index is not None:
        identifier += '_{}'.format(index)
    return identifier

def add_outputs_to_cluster(step,cluster):
    #add outputs circles
    for k,v in step.result.iteritems():
        for i,y in (enumerate(v) if type(v)==list else [(None,v)]):
            name = output_id(step.identifier,k,i)
            label = '{}[{}]: '.format(k,i) if i is not None else '{}: '.format(k)
            label += ' {}'.format(y)
            cluster.add_node(pydotplus.graphviz.Node(name, label = label, color = 'blue'))
            cluster.add_edge(pydotplus.graphviz.Edge(step.identifier,name))

def add_step_to_cluster(step,workflow,stagecluster,provgraph):
    step = workflow.dag.getNode(step)
    stepid = step.identifier

    pars = step.task.attributes.copy()
    parstrings =  [': '.join((k,str(pars[k]))) for k in sorted(pars.keys())]

    step_report = u'''\
{name}
______
{pars}
'''

    rep = step_report.format(name = step.name, pars = '\n'.join(parstrings))
    stagecluster.add_node(pydotplus.graphviz.Node(
                name = stepid,
                obj_dict = None,
                color = 'red',
                label = rep,
                shape = 'box'
        )
    )
    add_outputs_to_cluster(step,stagecluster)

    #connect node to outputs
    if step.task.inputs:
        # if input information is there, add edge to input
        for reference in step.task.inputs:
            refid = reference.stepid
            refptr = reference.pointer
            index =  refptr.parts[1] if len(refptr.parts)>1 else None
            provgraph.add_edge(pydotplus.graphviz.Edge(output_id(refid,refptr.parts[0],index),stepid))
    else:
        #if not, we'll just add to the dependent node directly
        for pre in workflow.dag.predecessors(stepid):
            log.warning('really no inputs to this node but predecessors?: %s',step)
            provgraph.add_edge(pydotplus.graphviz.Edge(pre,stepid))

def fillscope(cluster,workflow,scope = ''):
    # scopecluster = stagecluster = pydotplus.graphviz.Cluster(graph_name = '_'.join(stagescopeprts),
    scopecluster = pydotplus.graphviz.Cluster(
                        graph_name = scope.replace('/',''),
                        label = ''.join(['[{}]'.format(p) for p in scope.split('/')[1:]]),
                        style = 'solid',
                        color = 'blue')
    cluster.add_subgraph(scopecluster)
    scopeptr = jsonpointer.JsonPointer(scope)
    scoped = scopeptr.resolve(workflow.stepsbystage)
    for stage,elements in scoped.iteritems():
        stagescopeprts = scopeptr.parts+[stage]
        stagecluster = pydotplus.graphviz.Cluster(
            graph_name = '_'.join(stagescopeprts),
            label = stage,
            labeljust = 'l',
            color = 'grey',
            style = 'dashed')
        scopecluster.add_subgraph(stagecluster)
        for i,element in enumerate(elements):
            if '_nodeid' in element:
                element = element['_nodeid']
                targetcl = stagecluster if stage != 'init' else scopecluster
                shape = 'diamond' if stage == 'init' else 'box'
                label = '' if stage == 'init' else '{}[{}]'.format(stage,i)
                additional = {'fixedsize':True, 'height':0.2, 'width':0.2} if stage == 'init' else {}
                targetcl.add_node(pydotplus.graphviz.Node(element, label = label, color = 'blue', shape = shape,**additional))
                add_result(targetcl,element,workflow.dag.getNode(element).result)
            elif type(element)==dict:
                fillscope(stagecluster,workflow,jsonpointer.JsonPointer.from_parts(scopeptr.parts+[stage,i]).path)


def path_to_id(stepid,path):
    return '{}_{}'.format(stepid,path.replace('/','_'))

def add_result(graph,parent,jsondata):
    allleafs = jq.jq('leaf_paths').transform(jsondata, multiple_output=True)
    leafpointers = [jsonpointer.JsonPointer.from_parts(x) for x in allleafs]

    for leaf in leafpointers:
        leafid = path_to_id(parent,leaf.path)
        # value = leaf.resolve(jsondata)
        source = ''.join(['[{}]'.format(p) for p in leaf.parts])
        label = '{}'.format(source)
        graph.add_node(pydotplus.graphviz.Node(leafid, label = label, color = 'red'))
        graph.add_edge(pydotplus.graphviz.Edge(parent,leafid))

def attach_to_results(provgraph,workflow,node):
    for dep in workflow.dag.getNode(node).task.inputs:
        resultid =  path_to_id(dep.stepid,dep.pointer.path)
        provgraph.add_edge(pydotplus.graphviz.Edge(resultid,node))


def connect(provgraph,workflow):
    for node in workflow.dag.nodes():
        attach_to_results(provgraph,workflow,node)

def write_prov_graph(workdir,workflow,vizformat = 'pdf'):
    provgraph = pydotplus.graphviz.Graph()

    fillscope(provgraph,workflow)
    connect(provgraph,workflow)

    with open('{}/yadage_workflow_instance.dot'.format(workdir),'w') as dotfile:
        dotfile.write(provgraph.to_string())

    with open('{}/yadage_workflow_instance.{}'.format(workdir,vizformat),'w') as outfile:
        subprocess.call(shlex.split('dot -T{} {}/yadage_workflow_instance.dot'.format(vizformat,workdir)), stdout = outfile)

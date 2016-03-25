import shlex
import pydotplus
import networkx as nx
from networkx.drawing.nx_pydot import write_dot
import logging
import subprocess
log = logging.getLogger(__name__)

def simple_stage_graph(workflow):
    graph = nx.DiGraph()
    for stage in workflow['stages']:
        graph.add_node(stage['name'])
        for x in stage['dependencies']:
            graph.add_edge(x,stage['name'])
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
    for k,v in step.result_of().iteritems():
        for i,y in (enumerate(v) if type(v)==list else [(None,v)]):
            name = output_id(step.identifier,k,i)
            label = '{}[{}]: '.format(k,i) if i is not None else '{}: '.format(k)
            label += ' {}'.format(y)
            cluster.add_node(pydotplus.graphviz.Node(name, label = label, color = 'blue'))
            if not step.name=='init':
                cluster.add_edge(pydotplus.graphviz.Edge(step.identifier,name))
    
def add_step_to_cluster(step,adagegraph,cluster,fullgraph):
    stepid = step.identifier

    pars = step.task.attributes.copy()
    parstrings =  [': '.join((k,str(pars[k]))) for k in sorted(pars.keys())]

    step_report = u'''\
{name}
______
{pars}
'''

    rep = step_report.format(name = step.name, pars = '\n'.join(parstrings))
    if not step.name=='init':
        cluster.add_node(pydotplus.graphviz.Node(
                name = stepid,
                obj_dict = None,
                color = 'red',
                label = rep,
                shape = 'box'
            )
        )
    add_outputs_to_cluster(step,cluster)

    #connect node to outputs
    if step.task.inputs:
        #if input information is there, add edge to input
        for k,inputs_from_node in step.task.inputs.iteritems():
            for one in inputs_from_node:
                fullgraph.add_edge(pydotplus.graphviz.Edge(output_id(k,one[0],one[1]),stepid))
    else:
        #if not, we'll just add to the dependent node directly
        for pre in adagegraph.predecessors(stepid):
            log.warning('really no inputs to this node but predecessors?: %s',step)
            fullgraph.add_edge(pydotplus.graphviz.Edge(pre,stepid))

def write_prov_graph(workdir,adagegraph,workflow):
    provgraph = pydotplus.graphviz.Graph()
    
    stagedict = {stage['name']:stage for stage in workflow['stages']}
    for stage in nx.topological_sort(simple_stage_graph(workflow)):
        stagecluster = pydotplus.graphviz.Cluster(graph_name = stage, label = stage, labeljust = 'l', style = 'dotted')
        provgraph.add_subgraph(stagecluster)
        for step in stagedict[stage]['scheduled_steps']:
            add_step_to_cluster(step,adagegraph,stagecluster,provgraph)
            
    with open('{}/yadage_workflow_instance.dot'.format(workdir),'w') as dotfile:
        dotfile.write(provgraph.to_string())
    
    subprocess.call(shlex.split('dot -Tpdf {}/yadage_workflow_instance.dot'.format(workdir)),
                    stdout = open('{}/yadage_workflow_instance.pdf'.format(workdir),'w'))

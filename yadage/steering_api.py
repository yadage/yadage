#!/usr/bin/env python
import networkx as nx
from networkx.drawing.nx_pydot import write_dot
import adage
import adagebackend.backend
import logging
import subprocess
import os
import workflow_loader
import yaml
import shlex

log = logging.getLogger(__name__)

def write_stage_graph(workdir,workflow):
    stages_graph_simple = nx.DiGraph()
    for stage in workflow['stages']:
      stages_graph_simple.add_node(stage['name'])
      for x in stage['dependencies']:
        stages_graph_simple.add_edge(x,stage['name'])

    write_dot(stages_graph_simple,'{}/adage_stages.dot'.format(workdir))
    subprocess.call(['dot','-Tpdf','{}/adage_stages.dot'.format(workdir)], stdout = open('{}/adage_stages.pdf'.format(workdir),'w'))
    
def write_prov_graph(workdir,adagegraph):
    provgraph = nx.DiGraph()
    for x in nx.topological_sort(adagegraph):
        attr = adagegraph.node[x].copy()
        attr.update(color = 'red',label = adagegraph.getNode(x).name, shape = 'box')
        provgraph.add_node(x,attr)
        nodeinfo =  adagegraph.getNode(x).task
        
        #connect node to outputs
        #if input information is there, add edge to input
        if nodeinfo.inputs:
            for k,inputs_from_node in nodeinfo.inputs.iteritems():
                for one in inputs_from_node:
                    depname = 'output_{}_{}_{}'.format(k,one[0],one[1])
                    provgraph.add_edge(depname,x)

        #if not, we'll just add to the dependent node directly
        else:
            for pre in adagegraph.predecessors(x):
                provgraph.add_edge(pre,x)
        
        #add outputs circles
        for k,v in adagegraph.getNode(x).result_of().iteritems():
            for i,y in enumerate(v):
                name = 'output_{}_{}_{}'.format(adagegraph.getNode(x).task.step['name'],k,i)
                provgraph.add_node(name,{'label':'{}_{}: {}'.format(k,i,y),'color':'blue'})
                provgraph.add_edge(x,name)
        
    write_dot(provgraph,'{}/adage_workflow_instance.dot'.format(workdir))
    subprocess.call(shlex.split('dot -Tpdf {}/adage_workflow_instance.dot'.format(workdir)),
                    stdout = open('{}/adage_workflow_instance.pdf'.format(workdir),'w'))
    
def prepare_adage(workflow,global_context):
    stages_graph = nx.DiGraph()
    for stage in workflow['stages']:
      stages_graph.add_node(stage['name'],stage)
      for x in stage['dependencies']:
        stages_graph.add_edge(x,stage['name'])
    
    rules = {}
    for stagename in nx.topological_sort(stages_graph):
        stageinfo = stages_graph.node[stagename]
        rule = adagebackend.backend.yadage_rule(stageinfo,workflow,rules,global_context)
        rules[stagename] = rule
    
    g = adage.mk_dag()
    return g,rules

def run_workflow(workdir,analysis,context,loadtoplevel):
    log.info('running yadage workflow {}'.format(analysis))
    
    backend = adagebackend.backend.yadage_backend(2)
    
    context.update(workdir = workdir)
    
    for k,v in context.iteritems():
        candpath = '{}/inputs/{}'.format(workdir,v)
        if os.path.exists(candpath):
            context[k] = '/workdir/inputs/{}'.format(v)
            
    workflow = workflow_loader.workflow(analysis, toplevel = loadtoplevel)
    write_stage_graph(workdir,workflow)

    g, rules = prepare_adage(workflow,context)
    
    adage.rundag(g, rules.values(),
                 track = True,
                 backend = backend,
                 trackevery = 5,
                 workdir = workdir
                )

    write_prov_graph(workdir,g)
    log.info('finished yadage workflow {}'.format(analysis))
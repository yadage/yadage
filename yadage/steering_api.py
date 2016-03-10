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
      nodeinfo =  adagegraph.getNode(x).task.step

      if 'used_inputs' in nodeinfo:
        for k,inputs_from_node in nodeinfo['used_inputs'].iteritems():
          for one in inputs_from_node:
            depname = 'output_{}_{}_{}'.format(k,one[0],one[1])
            provgraph.add_edge(depname,x)
      else:
        for pre in adagegraph.predecessors(x):
          provgraph.add_edge(pre,x)


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

def run_workflow(workdir_local,workdir_global,analysis,context_yaml,loadtoplevel):
    log.info('running yadage workflow {}'.format(analysis))
    
    backend = adagebackend.backend.yadage_backend(2)
    
    global_context = yaml.load(open(context_yaml))
    global_context.update(global_workdir = workdir_global, workdir = workdir_local)
    
    for k,v in global_context.iteritems():
        candpath = '{}/inputs/{}'.format(workdir_local,v)
        if os.path.exists(candpath):
            global_context[k] = '/workdir/inputs/{}'.format(v)
            
    workflow = workflow_loader.workflow(analysis, toplevel = loadtoplevel)
    g, rules = prepare_adage(workflow,global_context)
    
    adage.rundag(g, rules.values(),
                 track = True,
                 backend = backend,
                 trackevery = 5,
                 workdir = workdir_local
                )
    
    write_prov_graph(workdir_local,g)
    write_stage_graph(workdir_local,workflow)
    log.info('finished yadage workflow {}'.format(analysis))
    
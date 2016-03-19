#!/usr/bin/env python
import networkx as nx
import adage
import adage.backends
from yadagerule import yadage_rule
import logging
import os
import workflow_loader
import visualize
log = logging.getLogger(__name__)

def prepare_adage(workflow,global_context):
    stages_graph = nx.DiGraph()
    for stage in workflow['stages']:
        stages_graph.add_node(stage['name'],stage)
        for x in stage['dependencies']:
            stages_graph.add_edge(x,stage['name'])
    
    rules = {}
    for stagename in nx.topological_sort(stages_graph):
        stageinfo = stages_graph.node[stagename]
        rule = yadage_rule(stageinfo,workflow,rules,global_context)
        rules[stagename] = rule
    
    g = adage.mk_dag()
    return g,rules

def run_workflow(workdir,analysis,context,loadtoplevel,loginterval):
    log.info('running yadage workflow %s',analysis)
    if not os.path.exists(workdir):
        raise RuntimeError('workdir %s does not exist',workdir)
    
    backend = adage.backends.MultiProcBackend(2)
    
    context.update(workdir = workdir)
    for k,v in context.iteritems():
        candpath = '{}/inputs/{}'.format(workdir,v)
        if os.path.exists(candpath):
            context[k] = '/workdir/inputs/{}'.format(v)
            
    workflow = workflow_loader.workflow(analysis, toplevel = loadtoplevel, schemadir = 'from-github')
    visualize.write_stage_graph(workdir,workflow)

    g, rules = prepare_adage(workflow,context)
    adage.rundag(g, rules.values(),
                 track = True,
                 backend = backend,
                 trackevery = loginterval,
                 workdir = workdir
                )

    visualize.write_prov_graph(workdir,g,workflow)
    log.info('finished yadage workflow %s',analysis)
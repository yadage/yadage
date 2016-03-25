#!/usr/bin/env python
import networkx as nx
import adage
import adage.backends
import yadagestep
from yadagerule import yadage_rule, init_rule
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
        if stagename=='init':
            rules[stagename] = init_rule(stageinfo,global_context['init'])
        else:
            rules[stagename] = yadage_rule(stageinfo,workflow,rules,global_context['yadage_ctx'])

    g = adage.mk_dag()
    return g,rules

def add_init_stage(workflow):
    """
    we add and initialization stage to the workflow
    """
    initstage = {
        'name':'init',
        'dependencies':[],
        'scheduler':None
    }
    workflow['stages'] = [initstage]+workflow['stages']

def run_workflow(workdir,analysis,context,loadtoplevel,loginterval,schemadir):
    """
    Main entry point to run a Yadage workflo
    """
    log.info('running yadage workflow %s',analysis)
    if not os.path.exists(workdir):
        raise RuntimeError('workdir %s does not exist',workdir)
    
    for k,v in context.iteritems():
        candpath = '{}/inputs/{}'.format(workdir,v)
        if os.path.exists(candpath):
            context[k] = '/workdir/inputs/{}'.format(v)


            
    workflow = workflow_loader.workflow(analysis, toplevel = loadtoplevel, schemadir = schemadir)
    run_adage(workdir,workflow,context,loginterval)
    log.info('finished yadage workflow %s',analysis)

def run_adage(workdir,workflow,context,loginterval):
    global_context = {
        'init':context,
        'yadage_ctx':{
            'workdir':workdir
        }
    }

    add_init_stage(workflow)
    
    visualize.write_stage_graph(workdir,workflow)
    g, rules = prepare_adage(workflow,global_context)
    
    backend = adage.backends.MultiProcBackend(2)
    
    adage.rundag(g, rules.values(),
                 track = True,
                 backend = backend,
                 trackevery = loginterval,
                 workdir = workdir
                )
    
    visualize.write_prov_graph(workdir,g,workflow)

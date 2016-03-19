import yaml
import re
import logging
import utils
import json
from yadage.yadagestep import yadagestep

log = logging.getLogger(__name__)

handlers,scheduler = utils.handler_decorator()

### A scheduler does the following things:
###   - attached new nodes to the DAG
###   - for each added step
###     - the step is given a name
###     - the step attributes are determined using the scheduler spec and context
###     - a list of used inputs (in the form of [stepname,outputkey,index])

@scheduler('single-from-ctx')
def single_step_from_context(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via single_step_from_context')
    stepname = '{}'.format(stage['name'])

    step = yadagestep(stepname,sched_spec['step'],context)

    attributes = utils.evaluate_parameters(stage['parameters'],context)
    print attributes

    node = dag.addTask(task = step.s(**attributes), nodename = stepname)
    stage['scheduled_steps'] = [node]

@scheduler('zip-from-dep')
def zip_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via zip_from_dep_output')

    used_nodes = []
    zipped_maps = []


    stepname = '{}'.format(stage['name'])
    step     = yadagestep(stepname,sched_spec['step'],context)
    
    ### we loop each zip pattern
    for zipconfig in sched_spec['zip']:
        new_inputs = []
        
        ### for each dependent stage we loop through its steps
        dependencies = [s for s in workflow['stages'] if s['name'] in zipconfig['from_stages']]
        for x in [s for d in dependencies for s in d['scheduled_steps']]:
            result = x.result_of()
            
            outputkey_regex = re.compile(zipconfig['outputs'])
            matching_outputkeys = [k for k in result.keys() if outputkey_regex.match(k)]
            
            ## for each step we loop through matching outputs
            for outputkey in matching_outputkeys:
                try:
                    for i,y in enumerate(result[outputkey]):
                        new_inputs += [y]
                        step.used_input(x.task.name,outputkey,i)
                        used_nodes += [x]
                except KeyError:
                    log.exception('could not fine output %s in metadata %s',outputkey,result)
        
        zipwith = zipconfig['zip_with']
        newmap = dict(zip(zipwith,new_inputs))
        log.debug('zipped map %s',newmap)
        zipped_maps += [newmap]
            
    attributes = utils.evaluate_parameters(stage['parameters'],context)
    for zipped in zipped_maps:
        attributes.update(**zipped)
    
    node = dag.addTask(step.s(**attributes), nodename = stepname)
    stage['scheduled_steps'] = [node]
    for x in used_nodes:
        dag.addEdge(x,node)
    
@scheduler('reduce-from-dep')
def reduce_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via reduce_from_dep_output')
    dependencies = [s for s in workflow['stages'] if s['name'] in sched_spec['from_stages']]
    
    new_inputs = []
    stepname = '{}'.format(stage['name'])
    step = yadagestep(stepname,sched_spec['step'],context)

    outputkey_regex = re.compile(sched_spec['outputs'])

    for x in [stp for d in dependencies for stp in d['scheduled_steps']]:
        result = x.result_of()
        log.debug('reduce_from_dep_output: matching %s to regex: %s',result.keys(),sched_spec['outputs'])
        matching_outputkeys = [k for k in result.keys() if outputkey_regex.match(k)]
        log.debug('reduce_from_dep_output: matching output keys %s',matching_outputkeys)
        for outputkey in matching_outputkeys:
            try:
                for i,y in enumerate(result[outputkey]):
                    new_inputs += [y]
                    step.used_input(x.task.name,outputkey,i)
            except KeyError:
                log.exception('could not fine output %s in metadata %s',outputkey,result)
    
    to_input = sched_spec['to_input']
    attributes = utils.evaluate_parameters(stage['parameters'],context)
    attributes[to_input] = new_inputs
    
    node = dag.addTask(step.s(**attributes), nodename = stepname)
    stage['scheduled_steps'] = [node]
    
    for x in [s for d in dependencies for s in d['scheduled_steps']]:
        dag.addEdge(x,node)
    
@scheduler('map-from-dep')
def map_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via map_from_dep_output')
    
    dependencies = [s for s in workflow['stages'] if s['name'] in sched_spec['from_stages']]
    
    outputkey           = sched_spec['outputs']
    to_input            = sched_spec['to_input']
    stepname_template   = stage['name']+'_{index}'
    stage['scheduled_steps'] = []
    index = 0
    
    outputkey_regex = re.compile(outputkey)
    
    for x in [step for d in dependencies for step in d['scheduled_steps']]:
        result = x.result_of()
        matching_outputs = [v for k,v in result.iteritems() if outputkey_regex.match(k)]
        
        for this_index,y in enumerate(out for thisout in matching_outputs for out in thisout):
            withindex = context.copy()
            withindex.update(index = index)
            attributes = utils.evaluate_parameters(stage['parameters'],withindex)
            attributes[to_input] = y
            
            step = yadagestep(stepname_template.format(index = index),sched_spec['step'],context)
            step.used_input(x.task.name,outputkey,this_index)
            
            node = dag.addTask(task = step.s(**attributes), nodename = step.name)
            dag.addEdge(x,node)
            stage['scheduled_steps'] += [node]
            index += 1

@scheduler('map-from-ctx')
def map_step_from_context(workflow,stage,dag,context,sched_spec):
    log.info('map_step_from_context')
    
    mappar = sched_spec['map_parameter']
    to_input = sched_spec['to_input']
    stepname_template   = stage['name']+'_{index}'
    
    allpars = utils.evaluate_parameters(stage['parameters'],context)
    parswithoutmap = stagepars.copy()
    parswithoutmap.pop(mappar)
    
    stage['scheduled_steps'] = []
    for index,p in enumerate(allpars[mappar]):
        withindex = context.copy()
        withindex.update(index = index)
        
        attributes = parswithoutmap
        attributes[to_input] = p

        step = yadagestep(stepname_template.format(index = index),sched_spec['step'],context)
        node = dag.addTask(task = step.s(**attributes), nodename = step.name)
        stage['scheduled_steps'] += [node]
    
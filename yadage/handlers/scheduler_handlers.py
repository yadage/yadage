import logging
import utils
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

    node = dag.addTask(task = step.s(**attributes), nodename = stepname)
    stage['scheduled_steps'] = [node]

@scheduler('zip-from-dep')
def zip_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via zip_from_dep_output')

    used_steps = []
    zipped_maps = []

    stepname = '{}'.format(stage['name'])
    step     = yadagestep(stepname,sched_spec['step'],context)
    
    ### we loop each zip pattern
    for zipconfig in sched_spec['zip']:
        
        ### for each dependent stage we loop through its steps
        dependencies = [s for s in workflow['stages'] if s['name'] in zipconfig['from_stages']]
        outputs = zipconfig['outputs']

        collected_inputs = []
        for depstep,outputkey,output_index in utils.regex_match_outputs(dependencies,[outputs]):
            output = depstep.result_of()[outputkey]
            collected_inputs += [output if not output_index else output[output_index]]
            step.used_input(depstep.task.name,outputkey,output_index)
            used_steps += [depstep]
            
        zipwith = zipconfig['zip_with']
        newmap = dict(zip(zipwith,collected_inputs))
        log.debug('zipped map %s',newmap)
        zipped_maps += [newmap]
            
    attributes = utils.evaluate_parameters(stage['parameters'],context)
    for zipped in zipped_maps:
        attributes.update(**zipped)
    
    node = dag.addTask(step.s(**attributes), nodename = stepname)
    stage['scheduled_steps'] = [node]
    for x in used_steps:
        dag.addEdge(x,node)
    
@scheduler('reduce-from-dep')
def reduce_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via reduce_from_dep_output')
    dependencies = [s for s in workflow['stages'] if s['name'] in sched_spec['from_stages']]
    
    stepname = '{}'.format(stage['name'])
    step = yadagestep(stepname,sched_spec['step'],context)

    outputs = sched_spec['outputs']

    collected_inputs = []
    used_steps = []
    for depstep,outputkey,output_index in utils.regex_match_outputs(dependencies,[outputs]):
        output = depstep.result_of()[outputkey]
        collected_inputs += [output if not output_index else output[output_index]]
        step.used_input(depstep.task.name,outputkey,output_index)
        used_steps += [depstep]

    to_input = sched_spec['to_input']
    attributes = utils.evaluate_parameters(stage['parameters'],context)
    attributes[to_input] = collected_inputs
    
    node = dag.addTask(step.s(**attributes), nodename = stepname)
    stage['scheduled_steps'] = [node]
    
    for x in used_steps:
        dag.addEdge(x,node)
    
@scheduler('map-from-dep')
def map_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via map_from_dep_output')
    
    dependencies = [s for s in workflow['stages'] if s['name'] in sched_spec['from_stages']]
    
    outputs           = sched_spec['outputs']
    to_input          = sched_spec['to_input']
    stepname_template = stage['name']+'_{index}'
    stage['scheduled_steps'] = []

    for index,(depstep,outputkey,output_index) in enumerate(utils.regex_match_outputs(dependencies,[outputs])):
        withindex = context.copy()
        withindex.update(index = index)
        attributes = utils.evaluate_parameters(stage['parameters'],withindex)

        output = depstep.result_of()[outputkey]
        attributes[to_input] = output if not output_index else output[output_index]

        step = yadagestep(stepname_template.format(index = index),sched_spec['step'],context)
        step.used_input(depstep.task.name,outputkey,output_index)
            
        node = dag.addTask(task = step.s(**attributes), nodename = step.name)
        
        #adding an edge is idempotent, so we don't care if we add it twice (if multiple inputs from on depstep)
        dag.addEdge(depstep,node)
        stage['scheduled_steps'] += [node]


@scheduler('map-from-ctx')
def map_step_from_context(workflow,stage,dag,context,sched_spec):
    log.info('map_step_from_context')
    
    mappar = sched_spec['map_parameter']
    to_input = sched_spec['to_input']
    stepname_template = stage['name']+'_{index}'
    
    allpars = utils.evaluate_parameters(stage['parameters'],context)
    parswithoutmap = allpars.copy()
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
    
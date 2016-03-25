import logging
import utils
from yadage.yadagestep import yadagestep

log = logging.getLogger(__name__)

handlers,scheduler = utils.handler_decorator()

### A scheduler does the following things:
###   - creates new tasks (yadagesteps)
###   - figures out attributs to call the task with
###   - keeps track of used inputs
###   - attaches the nodes for this task to the DAG

@scheduler('zip-from-dep')
def zip_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via zip_from_dep_output')

    zipped_maps = []

    stepname = '{}'.format(stage['name'])
    task     = yadagestep(stepname,sched_spec['step'],context)
    
    ### we loop each zip pattern
    for zipconfig in sched_spec['zip']:
        ### for each dependent stage we loop through its steps
        dependencies = [s for s in workflow['stages'] if s['name'] in zipconfig['from_stages']]
        outputs = zipconfig['outputs']

        refgen = utils.regex_match_outputs(dependencies,[outputs])
        collected_inputs = [utils.read_input(dag,task,reference) for reference in refgen]
                    
        zipwith = zipconfig['zip_with']
        newmap = dict(zip(zipwith,collected_inputs))
        log.debug('zipped map %s',newmap)
        zipped_maps += [newmap]
            
    attributes = utils.evaluate_parameters(stage['parameters'],context)
    for zipped in zipped_maps:
        attributes.update(**zipped)
    
    node = utils.addTask(dag,task.s(**attributes))
    stage['scheduled_steps'] = [node]
    
@scheduler('reduce-from-dep')
def reduce_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via reduce_from_dep_output')
    dependencies = [s for s in workflow['stages'] if s['name'] in sched_spec['from_stages']]
    
    stepname = '{}'.format(stage['name'])
    task = yadagestep(stepname,sched_spec['step'],context)

    outputs = sched_spec['outputs']

    refgen = utils.regex_match_outputs(dependencies,[outputs])
    collected_inputs = [utils.read_input(dag,task,reference) for reference in refgen]
    
    to_input = sched_spec['to_input']
    attributes = utils.evaluate_parameters(stage['parameters'],context)
    attributes[to_input] = collected_inputs

    node = utils.addTask(dag,task.s(**attributes))
    stage['scheduled_steps'] = [node]
    
@scheduler('map-from-dep')
def map_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via map_from_dep_output')
    
    dependencies = [s for s in workflow['stages'] if s['name'] in sched_spec['from_stages']]
    
    outputs           = sched_spec['outputs']
    to_input          = sched_spec['to_input']
    stepname_template = stage['name']+' {index}'
    stage['scheduled_steps'] = []

    for index,reference in enumerate(utils.regex_match_outputs(dependencies,[outputs])):
        withindex = context.copy()
        withindex.update(index = index)

        task = yadagestep(stepname_template.format(index = index),sched_spec['step'],context)

        attributes = utils.evaluate_parameters(stage['parameters'],withindex)
        attributes[to_input] = utils.read_input(dag,task,reference)

        node = utils.addTask(dag,task.s(**attributes))
        stage['scheduled_steps'] += [node]
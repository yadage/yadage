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

@scheduler('zip-from-dep')
def zip_from_dep_output(stage,spec):
    log.info('scheduling via zip_from_dep_output')

    zipped_maps = []

    stepname = stage.name
    task     = yadagestep(stepname,spec['step'],stage.context)
    
    ### we loop each zip pattern
    for zipconfig in spec['zip']:
        ### for each dependent stage we loop through its steps
        dependencies = [stage.workflow.stage(name) for name in zipconfig['from_stages']]
        outputs = zipconfig['outputs']

        collected_inputs = []
        for output,reference in utils.regex_match_outputs(dependencies,[outputs]):
            collected_inputs += [output]
            task.used_input(*reference)
            
        zipwith = zipconfig['zip_with']
        newmap = dict(zip(zipwith,collected_inputs))
        log.debug('zipped map %s',newmap)
        zipped_maps += [newmap]
            
    attributes = utils.evaluate_parameters(stage.stageinfo['parameters'],stage.context)
    for zipped in zipped_maps:
        attributes.update(**zipped)
    
    stage.addStep(task.s(**attributes))
    
    
@scheduler('reduce-from-dep')
def reduce_from_dep_output(stage,spec):
    log.info('scheduling via reduce_from_dep_output')
    dependencies = [stage.workflow.stage(name) for name in spec['from_stages']]
        
    stepname = stage.name
    task = yadagestep(stepname,spec['step'],stage.context)

    outputs = spec['outputs']

    collected_inputs = []
    for output,reference in utils.regex_match_outputs(dependencies,[outputs]):
        collected_inputs += [output]
        task.used_input(*reference)

    to_input = spec['to_input']
    attributes = utils.evaluate_parameters(stage.stageinfo['parameters'],stage.context)
    attributes[to_input] = collected_inputs

    stage.addStep(task.s(**attributes))
    

@scheduler('map-from-dep')
def map_from_dep_output(stage,spec):
    log.info('scheduling via map_from_dep_output')
    
    dependencies = [stage.workflow.stage(name) for name in spec['from_stages']]
    
    outputs           = spec['outputs']
    to_input          = spec['to_input']
    stepname_template = stage.name+'_{index}'

    for index,(output,reference) in enumerate(utils.regex_match_outputs(dependencies,[outputs])):
        withindex = stage.context.copy()
        withindex.update(index = index)
        attributes = utils.evaluate_parameters(stage.stageinfo['parameters'],withindex)
        attributes[to_input] = output

        task = yadagestep(stepname_template.format(index = index),spec['step'],stage.context)
        task.used_input(*reference)
        stage.addStep(task.s(**attributes))
        
@scheduler('single-from-ctx')
def single_step_from_context(stage,spec):
    log.info('scheduling via single_step_from_context')
    stepname = stage.name

    task = yadagestep(stepname,spec['step'],stage.context)
    attributes = utils.evaluate_parameters(stage.stageinfo['parameters'],stage.context)

    stage.addStep(task.s(**attributes))

@scheduler('map-from-ctx')
def map_step_from_context(stage,spec):
    log.info('map_step_from_context')
    
    mappar   = spec['map_parameter']
    to_input = spec['to_input']
    stepname_template = stage.name+'_{index}'
    
    allpars = utils.evaluate_parameters(stage.stageinfo['parameters'],stage.context)
    parswithoutmap = allpars.copy()
    parswithoutmap.pop(mappar)
    
    for index,p in enumerate(allpars[mappar]):
        withindex = stage.context.copy()
        withindex.update(index = index)
        attributes = parswithoutmap
        attributes[to_input] = p
        
        task = yadagestep(stepname_template.format(index = index),spec['step'],stage.context)
        stage.addStep(task.s(**attributes))
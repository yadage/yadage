import adage
import yaml
import re
import logging
import utils
from yadage.adagebackend.yadagestep import yadagestep
log = logging.getLogger(__name__)

handlers,scheduler = utils.handler_decorator()


### A scheduler does the following things:
###   - attached new nodes to the DAG
###   - for each added step
###     - the step is given a name
###     - the step attributes are determined using the scheduler spec and context
###     - a list of used inputs (in the form of [stepname,outputkey,index])

@scheduler('singlestep-from-context')
def single_step_from_context(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via single_step_from_context')
    stepname = '{}_single'.format(stage['name'])
    step = {
        'name':stepname,
        'attributes': {k:v.format(**context) for k,v in stage['parameters'].iteritems()},
        'step_spec':sched_spec['steps']['single']
    }
    
    step = adage.mknode(dag,task = yadagestep.s(step,context), nodename = stepname)
    stage['scheduled_steps'] = [step]

@scheduler('zip-from-dep-output')
def zip_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via zip_from_dep_output')

    used_inputs = {}
    used_nodes = []
    zipped_maps = []

    ### we loop each zip pattern
    for zipconfig in sched_spec['zip']:
        new_inputs = []

        ### for each dependent stage we loop through its steps
        dependencies = [s for s in workflow['stages'] if s['name'] in zipconfig['from_stages']]
        for x in [step for d in dependencies for step in d['scheduled_steps']]:
            rcmeta = x.result_of()['yadage_metadata']
            if not x.task.step['name'] in used_inputs:
                used_inputs[x.task.step['name']] = []

            outputkey_regex = re.compile(zipconfig['outputs'])
            matching_outputkeys = [k for k in rcmeta['outputs'].keys() if outputkey_regex.match(k)]

            ## for each step we loop through matching outputs
            for outputkey in matching_outputkeys:
                try:
                    for i,y in enumerate(rcmeta['outputs'][outputkey]):
                        new_inputs += [y]
                        used_inputs[x.task.step['name']] += [(outputkey,i)]
                        used_nodes += [x]
                except KeyError:
                    log.exception('could not fine output {} in metadata {}'.format(outputkey,rcmeta))

        zipwith = zipconfig['zip_with']
        newmap = dict(zip(zipwith,new_inputs))
        log.info('zipped map {}'.format(newmap))
        zipped_maps += [newmap]
            

    attributes = {k:str(v).format(**context) for k,v in stage['parameters'].iteritems()}
    for zipped in zipped_maps:
        attributes.update(**zipped)
    
    stepname = '{}_zipped'.format(stage['name'])
    step = {
        'name':stepname,
        'attributes':attributes,
        'step_spec':sched_spec['steps']['single'],
        'used_inputs':used_inputs
    }

    step = adage.mknode(dag,yadagestep.s(step,context), nodename = stepname)
    stage['scheduled_steps'] = [step]
    for x in used_nodes:
        dag.addEdge(x,step)
    
@scheduler('reduce-from-dep-output')
def reduce_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via reduce_from_dep_output')
    dependencies = [s for s in workflow['stages'] if s['name'] in sched_spec['from_stages']]

    new_inputs = []
    used_inputs = {}

    for x in [step for d in dependencies for step in d['scheduled_steps']]:
        used_inputs[x.task.step['name']] = []

        outputkey_regex = re.compile(sched_spec['take_outputs'])

        rcmeta = x.result_of()['yadage_metadata']
        log.info('reduce_from_dep_output: matching {} to regex: {}'.format(rcmeta['outputs'].keys(),sched_spec['take_outputs']))
        matching_outputkeys = [k for k in rcmeta['outputs'].keys() if outputkey_regex.match(k)]
        log.info('reduce_from_dep_output: matching output keys {}'.format(matching_outputkeys))
        for outputkey in matching_outputkeys:
            try:
                rcmeta = x.result_of()['yadage_metadata']
                for i,y in enumerate(rcmeta['outputs'][outputkey]):
                    new_inputs += [y]
                    used_inputs[x.task.step['name']] += [(outputkey,i)]
            except KeyError:
                log.exception('could not fine output {} in metadata {}'.format(outputkey,rcmeta))

    to_input = sched_spec['to_input']
    attributes = {k:str(v).format(**context) for k,v in stage['parameters'].iteritems()}
    attributes[to_input] = new_inputs
    
    stepname = '{}_reduce'.format(stage['name'])
    step = {
        'name':stepname,
        'attributes':attributes,
        'step_spec':sched_spec['steps']['reduce'],
        'used_inputs':used_inputs
    }

    step = adage.mknode(dag,yadagestep.s(step,context), nodename = stepname)
    stage['scheduled_steps'] = [step]
    for x in [s for d in dependencies for s in d['scheduled_steps']]:
        dag.addEdge(x,step)

@scheduler('map-from-dep-output')
def map_from_dep_output(workflow,stage,dag,context,sched_spec):
    log.info('scheduling via map_from_dep_output')
    dependencies = [s for s in workflow['stages'] if s['name'] in sched_spec['from_stages']]
    
    outputkey           = sched_spec['take_outputs']
    to_input            = sched_spec['to_input']
    stepname_template   = sched_spec['stepname']
    stage['scheduled_steps'] = []
    index = 0

    outputkey_regex = re.compile(outputkey)
    
    for x in [step for d in dependencies for step in d['scheduled_steps']]:
      rcmeta = x.result_of()['yadage_metadata']
      matching_outputs = [v for k,v in rcmeta['outputs'].iteritems() if outputkey_regex.match(k)]
      
      for this_index,y in enumerate(out for thisout in matching_outputs for out in thisout):
        withindex = context.copy()
        withindex.update(index = index)

        
        attributes = {k:str(v).format(**withindex) for k,v in stage['parameters'].iteritems()}
        attributes[to_input] = y
        
        used_inputs = {x.task.step['name'] :[(outputkey,this_index)]}

        step = {
          'name': stepname_template.format(index = index),
          'attributes': attributes,
          'step_spec':sched_spec['steps']['map'],
          'used_inputs':used_inputs
        }
        stepobj = adage.mknode(dag,task = yadagestep.s(step,context), nodename = step['name'])
        dag.addEdge(x,stepobj)
        stage['scheduled_steps'] += [stepobj]
        index += 1


@scheduler('map-from-context')
def map_step_from_context(workflow,stage,dag,context,sched_spec):
    log.info('map_step_from_context')
    mappar = sched_spec['map_parameter']
    to_input = sched_spec['to_input']
    stepname_template   = sched_spec['stepname']
    stagepars = stage['parameters']

    #for the one parameter pointed to by mappar, we interpret the string as a serialized yaml list
    mapparlist = yaml.load(stagepars[mappar].format(**context))
    
    #the remaining parameter set will be used for each scheduled step
    parswithoutmap = stage['parameters']
    parswithoutmap.pop(mappar)
    stage['scheduled_steps'] = []
    for index,p in enumerate(mapparlist):
        withindex = context.copy()
        withindex.update(index = index)
        
        attributes = {k:str(v).format(**withindex) for k,v in parswithoutmap.iteritems()}
        attributes[to_input] = p
        step = {
          'name': stepname_template.format(index = index),
          'attributes': attributes,
          'step_spec':sched_spec['steps']['map'],
        }
        stepobj = adage.mknode(dag,task = yadagestep.s(step,context), nodename = step['name'])
        stage['scheduled_steps'] += [stepobj]

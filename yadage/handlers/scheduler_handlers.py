import logging
import utils
import jsonpointer
import itertools
import os

from yadage.yadagestep import yadagestep, initstep, outputReference
from yadage.yadagemodels import jsonstage

log = logging.getLogger(__name__)

handlers,scheduler = utils.handler_decorator()

### A scheduler does the following things:
###   - attached new nodes to the DAG
###   - for each added step
###     - the step is given a name
###     - the step attributes are determined using the scheduler spec and context
###     - a list of used inputs (in the form of [stepname,outputkey,index])

def resolve_output(step,selection):
    return (step.identifier,jsonpointer.JsonPointer.from_parts([selection]),step.result[selection])

def combine_outputs(outputs,flatten,unwrapsingle):
    combined = []
    for stepid, pointer, result in outputs:
        if type(result)==list:
            if flatten: 
                for i in range(len(result)):
                    deeppointer = jsonpointer.JsonPointer.from_parts(pointer.parts+[i])
                    combined+=[outputReference(stepid,jsonpointer.JsonPointer.from_parts(pointer.parts+[i]))]
            else:
                combined+=[outputReference(stepid,pointer)]
        else:
            combined+=[outputReference(stepid,pointer)]
    if len(combined)==1 and unwrapsingle:
        combined = combined[0]
    return combined

def select_steps(stage,query):
    return stage.view.getSteps(query)

def select_outputs(steps,selection,flatten,unwrapsingle):
    return combine_outputs(map(lambda s: resolve_output(s,selection),steps),flatten,unwrapsingle)

def resolve_reference(stage,selection):
    if type(selection) is not dict:
        return None
    else:
        steps   = select_steps(stage, selection['stages'])
        outputs = select_outputs(steps, selection['output'], selection.get('flatten',False), selection.get('unwrap',False))
        return outputs

def select_parameter(stage,parameter):
    if type(parameter) is not dict:
        value = parameter
    else:
        value = resolve_reference(stage,parameter)
    return value


def finalize_value(stage,step,value,context):
    if type(value)==outputReference:
        step.used_input(value)
        v = value.pointer.resolve(stage.view.dag.getNode(value.stepid).result)
        return finalize_value(stage,step,v,context)
    if type(value)==list:
        return [finalize_value(stage,step,v,context) for v in value]
    if type(value)==str:
        return value.format(**context)
    return value
    
def finalize_input(stage,step,json,context):
    context = context.copy()
    context['workdir'] = context['readwrite'][0]
    result = {}
    for k,v in json.iteritems():
        if type(v) is not list:
            result[k] = finalize_value(stage,step,v,context)
        else:
            result[k] = [finalize_value(stage,step,element,context) for element in v]
    return result

def step_or_init(name,spec,context):
    if 'step' in spec:
        return yadagestep(name = name, spec = spec['step'], context = context)
    elif 'workflow' in spec:
        return initstep('init {}'.format(name))

def addStepOrWorkflow(name,stage,step,spec):
    if type(step)==initstep:
        newcontext = {'readwrite':['{}/{}'.format(stage.context['readwrite'][0],name)], 'readonly':[]}
        newcontext['readonly'] += [ro for ro in itertools.chain(stage.context['readonly'],stage.context['readwrite'])]
        os.makedirs(newcontext['readwrite'][0])
        subrules = [jsonstage(yml,newcontext) for yml in spec['workflow']['stages']]
        stage.addWorkflow(subrules, initstep = step, offset = name)
    else:
        stage.addStep(step)

@scheduler('singlestep-stage')
def simple_stage(stage,spec):
    
    parameters = {
        k:select_parameter(stage,v) for k,v in spec['parameters'].iteritems()
    }

    step = step_or_init(name = stage.name, spec = spec, context = stage.context)
    finalized = finalize_input(stage,step,parameters,stage.context)

    addStepOrWorkflow(stage.name,stage,step.s(**finalized),spec)        
    
def scatter(parameters,scatter):
    steps = []
    commonpars = parameters.copy()
    to_scatter = {}
    for scatpar in scatter['parameters']:
        to_scatter[scatpar] = commonpars.pop(scatpar)
    
    singlesteppars=[]
    if scatter['method']=='zip':
        keys, zippable = zip(*[(k,v) for i,(k,v) in enumerate(to_scatter.iteritems())])

        for zipped in zip(*zippable):
            individualpars = dict(zip(keys,zipped))
            pars = commonpars.copy()
            pars.update(**individualpars)
            singlesteppars += [pars]

    if scatter['method']=='cartesian':
        for what in itertools.product(*[to_scatter[k] for k in scatter['parameters']]):
            individualpars = dict(zip(scatter['keys'],what))
            pars = commonpars.copy()
            pars.update(**individualpars)
            singlesteppars += [pars]
    return singlesteppars

@scheduler('multistep-stage')
def multi_stage(stage,spec):
    parameters = {
        k:select_parameter(stage,v) for k,v in spec['parameters'].iteritems()
    }
    singlesteppars = scatter(parameters,spec['scatter'])

    for i,pars in enumerate(singlesteppars):
        index_context = stage.context.copy()
        index_context.update(index = i)
        
        singlename = '{}_{}'.format(stage.name,i)
        step = step_or_init(name = singlename, spec = spec, context = stage.context)
        finalized = finalize_input(stage,step,pars,index_context)
        
        addStepOrWorkflow(singlename,stage,step.s(**finalized),spec)        

    
    

    

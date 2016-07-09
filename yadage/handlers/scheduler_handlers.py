import logging
import utils
import jsonpointer
import itertools
import os
import copy
import jq
import jsonpath_rw

from yadage.yadagestep import yadagestep, initstep, outputReference
from yadage.yadagemodels import jsonStage

log = logging.getLogger(__name__)

handlers,scheduler = utils.handler_decorator()

### A scheduler does the following things:
###   - attached new nodes to the DAG
###   - for each added step
###     - the step is given a name
###     - the step attributes are determined using the scheduler spec and context
###     - a list of used inputs (in the form of [stepname,outputkey,index])

def pointerize(jsondata, asref = False, stepid = None):
    allleafs = jq.jq('leaf_paths').transform(jsondata, multiple_output=True)
    leafpointers = [jsonpointer.JsonPointer.from_parts(x).path for x in allleafs]
    jsondata_proxy = copy.deepcopy(jsondata)
    for leaf in leafpointers:
        x = jsonpointer.JsonPointer(leaf)
        x.set(jsondata_proxy, outputReference(stepid,x) if asref else x.path)
    return jsondata_proxy

def select_reference(step,selection):
    pointerized = pointerize(step.result, asref = True, stepid = step.identifier)
    matches = jsonpath_rw.parse(selection).find(pointerized)
    if not matches:
        log.error('no matches found for selection %s in result %s',selection,step.result)
        raise RuntimeError('no matches found in reference selection')


    if len(matches) > 1:
        log.error('found multiple matches to query: %s within result: %s\n \ matches %s',selection,step.result,matches)
        raise RuntimeError('multiple matches in result jsonpath query')
    return matches[0].value

def combine_outputs(outputs,flatten,unwrapsingle):
    combined = []
    for reference in outputs:
        if type(reference)==list:
            if flatten:
                for elementref in reference:
                    combined+=[elementref]
            else:
                combined+=[reference]
        else:
            combined+=[reference]
    if len(combined)==1 and unwrapsingle:
        combined = combined[0]
    return combined

def select_steps(stage,query):
    return stage.view.getSteps(query)

def select_outputs(steps,selection,flatten,unwrapsingle):
    return combine_outputs(map(lambda s: select_reference(s,selection),steps),flatten,unwrapsingle)

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
        return [finalize_value(stage,step,x,context) for x in value]
    if type(value) in [str,unicode]:
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

def make_new_context(name,oldcontext):
    newcontext = {'readwrite':['{}/{}'.format(oldcontext['readwrite'][0],name)], 'readonly':[]}
    newcontext['readonly'] += [ro for ro in itertools.chain(oldcontext['readonly'],oldcontext['readwrite'])]
    os.makedirs(newcontext['readwrite'][0])
    return newcontext

def addStepOrWorkflow(name,stage,step,spec):
    if type(step)==initstep:
        newcontext = make_new_context(name,stage.context)
        subrules = [jsonStage(yml,newcontext) for yml in spec['workflow']['stages']]
        stage.addWorkflow(subrules, initstep = step)
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

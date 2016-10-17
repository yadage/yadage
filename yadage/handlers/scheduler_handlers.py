import logging
import utils
import itertools

from expression_handlers import handlers as exprhandlers
from yadage.yadagestep import yadagestep, initstep, outputReference
from yadage.yadagemodels import jsonStage
import  packtivity.statecontexts.poxisfs_context as statecontext

log = logging.getLogger(__name__)

handlers, scheduler = utils.handler_decorator()

# A scheduler does the following things:
#   - attached new nodes to the DAG
# - for each added step
#     - the step is given a name
#     - the step attributes are determined using the scheduler spec and context
#     - a list of used inputs (in the form of [stepname,outputkey,index])

def select_parameter(stageview,parameter):
    if type(parameter) is not dict:
        value = parameter
    else:
        handler = exprhandlers[parameter['expression_type']]
        value = handler(stageview,parameter)
    return value

def finalize_value(stage,step,value,context):
    '''
    finalize a value by recursively resolving references and
    interpolating with the context when necessary
    '''
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
    '''
    evaluate final values of parameters by either resolving a
    reference to a upstream output or evaluating a static
    reference from the template (possibly string-interpolated)
    '''

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
        stepcontext = statecontext.make_new_context(name,context, subdir = True)
        return yadagestep(name = name, spec = spec['step'], context = stepcontext)
    elif 'workflow' in spec:
        return initstep('init {}'.format(name))


def addStepOrWorkflow(name,stage,step,spec):
    if type(step)==initstep:
        newcontext = statecontext.make_new_context(name,stage.context)
        subrules = [jsonStage(yml,newcontext) for yml in spec['workflow']['stages']]
        stage.addWorkflow(subrules, initstep = step)
    else:
        stage.addStep(step)

def get_parameters(spec):
    return {x['key']:x['value']for x in spec['parameters']}

@scheduler('singlestep-stage')
def singlestep_stage(stage,spec):
    '''
    a simple state that adds a single step/workflow. The node is attached
    to the DAG based on used upstream outputs
    '''
    log.debug('scheduling singlestep stage with spec:\n%s',spec)

    step = step_or_init(name = stage.name, spec = spec, context = stage.context)
    ctx = step.context if hasattr(step,'context') else stage.context

    parameters = {
        k:select_parameter(stage.view,v) for k,v in get_parameters(spec).iteritems()
    }
    finalized = finalize_input(stage,step,parameters,ctx)

    addStepOrWorkflow(stage.name,stage,step.s(**finalized),spec)

def scatter(parameters,scatter):
    '''
    this method turns a parameter set and scatter definition into a list
    of single parameter sets.
    '''
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
def multistep_stage(stage,spec):
    '''
    a stage that attaches an array of nodes to the DAG. The number of nodes
    is determined by a scattering recipe. Currently two algs are supported
    'zip': one or more arrays of length n are iterated through in lock-step.
           n nodes are added to the DAG where the  parameters values are set to
           the values in the iteration
    'cartesian': a cartesian product of a number of arrays (possibly different sizes)
                 adds n1 x n2 x ... nj nodes.
    Nodes are attached to the DAG based on used upstream inputs
    '''
    log.debug('scheduling multistep stage with spec:\n%s',spec)
    parameters = {
        k:select_parameter(stage.view,v) for k,v in get_parameters(spec).iteritems()
    }
    singlesteppars = scatter(parameters,spec['scatter'])
    for i,pars in enumerate(singlesteppars):
        singlename = '{}_{}'.format(stage.name,i)
        step = step_or_init(name = singlename, spec = spec, context = stage.context)
        ctx = step.context if hasattr(step,'context') else stage.context
        ctx = ctx.copy()
        ctx.update(index = i)
        finalized = finalize_input(stage,step,pars,ctx)
        addStepOrWorkflow(singlename,stage,step.s(**finalized),spec)

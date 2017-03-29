import logging
import utils
import itertools
import copy

import packtivity.statecontexts.posixfs_context as statecontext

from expression_handlers import handlers as exprhandlers
from yadage.yadagestep import yadagestep, initstep, outputReference
from yadage.yadagemodels import jsonStage
from yadage.helpers import leaf_iterator

log = logging.getLogger(__name__)

handlers, scheduler = utils.handler_decorator()

# A scheduler does the following things:
#   - attached new nodes to the DAG
# - for each added step
#     - the step is given a name
#     - the step attributes are determined using the scheduler spec and context
#     - a list of used inputs (in the form of [stepname,outputkey,index])


def select_parameter(wflowview, parameter):
    '''
    Evaluates parameter expressions (if needed) in the context of a workflow view

    :param wflowview: the workflow view on which to evaluete possible value expressions
    :param parameter: either a non-dict value or a JSON-like dict for a
                      supported value expression
    :return: the parameter value
    '''
    if type(parameter) is not dict:
        value = parameter
    else:
        handler = exprhandlers[parameter['expression_type']]
        value = handler(wflowview, parameter)
    return value

def finalize_value(wflowview, step, value, context):
    '''
    finalize a value by recursively resolving references and
    contextualizing it for the passed state context

    :param wflowview: the workflow view against which to resolve upstream references
    :param step: the step for which to track usage of upstream references
    :param value: the parameter value. May be a output reference, or a JSON value type
    :param context: the state context used to contextualize parameter values
    :return: finalized parameter value
    '''
    if type(value) == outputReference:
        step.used_input(value)
        v = value.pointer.resolve(wflowview.dag.getNode(value.stepid).result)
        return finalize_value(wflowview, step, v, context)
    return statecontext.contextualize_data(value,context)


def finalize_input(wflowview, step, jsondata, context):
    '''
    evaluate final values of step parameters by either resolving a
    reference to a upstream output and contextualizing stateful
    parameters. Also tracks usage of upstream references for the step

    :param wflowview: the workflow view view against which to resolve any upstream references
    :param step: the step that for which to track usage of upstream references
    :param jsondata: the prospective step parameters
    :param context: the state context 

    :return: finalized step parameters
    '''
    result = copy.deepcopy(jsondata)
    for leaf_pointer, leaf_value in leaf_iterator(jsondata):
        leaf_pointer.set(result,finalize_value(wflowview, step, leaf_value, context))
    return result

def step_or_init(name, spec, context):
    '''
    create a named yadagestep of sub-workflow initstep object based on stage spec

    :param name: name of the eventual (init-)step
    :param spec: the stage spec
    :param context: the stage's state context

    :return: yadage or init step object
    '''
    if 'step' in spec:
        stepcontext = statecontext.make_new_context(name, context, subdir=True)
        return yadagestep(name=name, spec=spec['step'], context=stepcontext)
    elif 'workflow' in spec:
        return initstep('init {}'.format(name))


def addStepOrWorkflow(name, stage, step, spec):
    '''
    adds a step or a sub-workflow init step to the current workflow view based on a stage
    
    :param str name: the name of the step or sub-workflow
    :param stage: the stage from which to use state context and workflow view
    :param step: either a yadagestep (for normal workflow steps) initstep object (for sub-workflows)
    :param spec: the stage spec

    :return: None
    '''
    if type(step) == initstep:
        newcontext = statecontext.make_new_context(name, stage.context)
        subrules = [jsonStage(yml, newcontext)
                    for yml in spec['workflow']['stages']]
        stage.addWorkflow(subrules, initstep=step)
    else:
        stage.addStep(step)

def get_parameters(spec):
    '''
    retrieve parameters from the spec

    :param spec: the stage spec
    :return: a JSON-like object of stage parameters
    '''
    return {x['key']: x['value']for x in spec['parameters']}

@scheduler('singlestep-stage')
def singlestep_stage(stage, spec):
    '''
    a simple state that adds a single step/workflow. The node is attached
    to the DAG based on used upstream outputs
    
    :param stage: common stage parent object 
    :param spec: stage JSON-like spec
    
    :return: None
    '''
    log.debug('scheduling singlestep stage with spec:\n%s', spec)

    step = step_or_init(name=stage.name, spec=spec, context=stage.context)
    ctx = step.context if hasattr(step, 'context') else stage.context

    parameters = {
        k: select_parameter(stage.view, v) for k, v in get_parameters(spec).iteritems()
    }
    finalized = finalize_input(stage.view, step, parameters, ctx)
    addStepOrWorkflow(stage.name, stage, step.s(**finalized), spec)

def scatter(parameters, scatter):
    '''
    convert a parameter set and scatter definition into a list
    of single parameter sets.
    :param parameters: the parameter definition
    :param scatter: scattering method. One of 'zip' or 'cartesian'
    :return: list of parameter sets
    '''
    commonpars = parameters.copy()
    to_scatter = {}
    for scatpar in scatter['parameters']:
        to_scatter[scatpar] = commonpars.pop(scatpar)

    singlesteppars = []
    if scatter['method'] == 'zip':
        keys, zippable = zip(
            *[(k, v) for i, (k, v) in enumerate(to_scatter.iteritems())])

        for zipped in zip(*zippable):
            individualpars = dict(zip(keys, zipped))
            pars = commonpars.copy()
            pars.update(**individualpars)
            singlesteppars += [pars]

    if scatter['method'] == 'cartesian':
        for what in itertools.product(*[to_scatter[k] for k in scatter['parameters']]):
            individualpars = dict(zip(scatter['keys'], what))
            pars = commonpars.copy()
            pars.update(**individualpars)
            singlesteppars += [pars]
    return singlesteppars


@scheduler('multistep-stage')
def multistep_stage(stage, spec):
    '''
    a stage that attaches an array of nodes to the DAG. The number of nodes
    is determined by a scattering recipe. Currently two algs are supported

    - ``zip``: one or more arrays of length n are iterated through in lock-step.
       n nodes are added to the DAG where the  parameters values are set to
           the values in the iteration
    - ``cartesian``:  a cartesian product of a number of arrays (possibly different sizes)
       adds n1 x n2 x ... nj nodes.

    Nodes are attached to the DAG based on used upstream inputs

    :param stage: common stage parent object 
    :param spec: stage JSON-like spec
    
    :return: None
    '''
    log.debug('scheduling multistep stage with spec:\n%s', spec)
    parameters = {
        k: select_parameter(stage.view, v) for k, v in get_parameters(spec).iteritems()
    }
    singlesteppars = scatter(parameters, spec['scatter'])
    for i, pars in enumerate(singlesteppars):
        singlename = '{}_{}'.format(stage.name, i)
        step = step_or_init(name=singlename, spec=spec, context=stage.context)
        ctx = step.context if hasattr(step, 'context') else stage.context
        ctx = ctx.copy()
        ctx.update(index=i)
        finalized = finalize_input(stage.view, step, pars, ctx)
        addStepOrWorkflow(singlename, stage, step.s(**finalized), spec)

import copy
import itertools
import logging

import jq
import jsonpointer

import yadage.handlers.utils as utils

from ..stages import JsonStage
from ..tasks import packtivity_task
from ..utils import (init_stage_spec, leaf_iterator_jsonlike,
                     outputReference, process_jsonlike, pointerize)
from .expression_handlers import handlers as exprhandlers

log = logging.getLogger(__name__)

handlers, scheduler = utils.handler_decorator()

# A scheduler does the following things:
#   - attached new nodes to the DAG
# - for each added step
#     - the step is given a name
#     - the step parameters are determined using the scheduler spec and context
#     - a list of used inputs (in the form of [stepname,outputkey,index])

def isExpression(parameter):
    return type(parameter)==dict and 'expression_type' in parameter

def select_parameter(wflowview, parameter):
    '''
    Evaluates parameter expressions (if needed) in the context of a workflow view

    :param wflowview: the workflow view on which to evaluete possible value expressions
    :param parameter: either a non-dict value or a JSON-like dict for a
                      supported value expression
    :return: the parameter value
    '''
    log.debug('selecting parameter %s', parameter)
    if isExpression(parameter):
        handler = exprhandlers[parameter['expression_type']]
        value = handler(wflowview, parameter)
    else:
        value = parameter
    return value

def finalize_value(wflowview, value, inputs):
    '''
    finalize a value by recursively resolving references

    :param wflowview: the workflow view against which to resolve upstream references
    :param step: the step for which to track usage of upstream references
    :param value: the parameter value. May be a output reference, or a JSON value type
    :return: finalized parameter value
    '''
    if type(value) == outputReference:
        return wflowview.dag.getNode(value.stepid).readfromresult(value.pointer.path, inputs)
    else:
        return value

def finalize_input(jsondata,wflowview):
    '''
    evaluate final values of step parameters by resolving
    references to a upstream output and contextualizing stateful
    parameters. Also tracks usage of upstream references for the step

    :param wflowview: the workflow view view against which to resolve any upstream references
    :param step: the step that for which to track usage of upstream references
    :param jsondata: the prospective step parameters

    :return: finalized step parameters
    '''
    result = copy.deepcopy(jsondata)
    inputs = []
    for leaf_pointer, leaf_value in leaf_iterator_jsonlike(jsondata):
        v = finalize_value(wflowview, leaf_value, inputs)
        if leaf_pointer.path == '': return v,inputs #only one leaf
        leaf_pointer.set(result,v)
    return result, inputs

def step_or_stages(name, spec, inputs, parameters, state_provider, stageview):
    '''
    :param name: name of the eventual (init-)step
    :param spec: the stage spec
    :param state_provider: the stage's state provider
    :param dependencies: dependent nodes

    :return: yadage or init step object
    '''

    dependencies = [stageview.dag.getNode(k.stepid) for k in inputs]
    depstates = [d.task.state for d in set(dependencies) if d.task.state]

    if 'step' in spec:
        step_state = state_provider.new_state(name,depstates)
        p = packtivity_task(name=name, spec=spec['step'], parameters = parameters, state=step_state, inputs = inputs)
        return p,None
    elif 'workflow' in spec:
        opts = spec.get('workflow_opts',{})
        
        init_spec  = init_stage_spec(
            parameters.json(),
            discover = opts.get('discover',False),
            relative = opts.get('relative',True),
            used_inputs=[x.json() for x in inputs],
            name = 'init',
            nodename = 'init_{}'.format(name)
        )
        stages = [init_spec] + spec['workflow']['stages']
        new_provider = state_provider.new_provider(name, init_states = depstates)
        stageobjects = [JsonStage(s, new_provider) for s in stages]
        return None, stageobjects
    elif 'cases' in spec:
        for x in spec['cases']:
            if parameters.jq(x['if']).json():
                log.info('selected case %s', x['if'])
                return step_or_stages(name, x, inputs, parameters, state_provider, stageview)
        log.info('no case selected on pars %s', parameters)
        return None, None
    raise RuntimeError('do not know what kind of stage spec we are dealing with. %s' % spec.keys())

def registerExpressions(stage, expressions):
    if not expressions: return
    for key, expression in expressions.items():
        stage.view.addValue(key, expression)

def addStepOrWorkflow(name, stage, parameters, inputs, spec):
    '''
    adds a step or a sub-workflow based on a init step

    :param str name: the name of the step or sub-workflow
    :param stage: the stage from which to use state context and workflow view
    :param step: either a packtivity_task (for normal workflow steps) initstep object (for sub-workflows)
    :param spec: the stage spec

    :return: None
    '''
    step, stages = step_or_stages(name, spec, inputs, parameters, stage.state_provider, stage.view)
    if step:
        stage.addStep(step)
        log.debug('scheduled a step')

    if stages: #subworkflow case
        stage.addWorkflow(stages,isolate = True)
        log.debug('scheduled a subworkflow')

def get_parameters(parameters):
    '''
    retrieve parameters from the spec

    :param spec: the stage spec
    :return: a JSON-like object of stage parameters
    '''
    return {x['key']: x['value']for x in parameters}

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
    parameters = {
        k: select_parameter(stage.view, v) for k, v in get_parameters(spec['parameters']).items()
    }
    finalized, inputs = finalize_input(parameters, stage.view)
    finalized = stage.datamodel.create(finalized, getattr(stage.state_provider,'datamodel',None))
    addStepOrWorkflow(stage.name, stage, finalized, inputs, spec)
    registerExpressions(stage, spec.get('register_values'))

def chunk(alist, chunksize):
    '''split a list into equal-sized chunks of size chunksize'''
    return [alist[x:x+chunksize] for x in range(0, len(alist), chunksize)]


def partition(alist, partitionsize):
    '''split a list into partitionsize parts'''
    total_len = len(alist)
    if partitionsize > total_len:
        partitionsize = total_len
    assert partitionsize <= total_len
    end = 0
    partitioned = []
    for k in range(partitionsize):
        begin = end
        end =  end + (total_len+k)//partitionsize
        partitioned.append(alist[begin:end])
    return partitioned

def groupmany(iterable,batchsize = None, partitionsize = None):
    if batchsize:
        return chunk(list(iterable), batchsize)
    if partitionsize:
        return partition(list(iterable), partitionsize)
    return iterable


def scatter(parameters, scatter, batchsize = None, partitionsize = None):
    '''
    convert a parameter set and scatter definition into a list
    of single parameter sets.

    :param parameters: the parameter definition
    :param scatter: scattering method. One of 'zip' or 'cartesian'

    :return: list of parameter sets
    '''

    log.debug('scattering:  pars: %s scatter: %s batchsisze: %s partitionsize: %s',
        parameters, scatter, batchsize, partitionsize
    )
    commonpars = parameters.copy()
    to_scatter = {}
    for scatpar in scatter['parameters']:
        to_scatter[scatpar] = groupmany(commonpars.pop(scatpar), batchsize, partitionsize)

    singlesteppars = []
    if scatter['method'] == 'zip':
        keys, zippable = zip(*[(k, v) for k, v in to_scatter.items()])
        for zipped in zip(*zippable):
            individualpars = dict(zip(keys, zipped))
            pars = commonpars.copy()
            pars.update(**individualpars)
            singlesteppars += [pars]

    if scatter['method'] == 'cartesian':
        for what in itertools.product(*[to_scatter[k] for k in scatter['parameters']]):
            individualpars = dict(zip(scatter['parameters'], what))
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
        k: select_parameter(stage.view, v) for k, v in get_parameters(spec['parameters']).items()
    }
    singlesteppars = scatter(parameters, spec['scatter'], spec.get('batchsize'), spec.get('partitionsize'))

    for i, pars in enumerate(singlesteppars):
        singlename = '{}_{}'.format(stage.name, i)
        finalized, inputs = finalize_input(pars, stage.view)
        finalized = stage.datamodel.create(finalized, getattr(stage.state_provider,'datamodel',None))
        addStepOrWorkflow(singlename, stage, finalized, inputs, spec)
    registerExpressions(stage, spec.get('register_values'))


def process_noderef(leafobj,resultscript,view):
    n = view.dag.getNode(leafobj['_nodeid'])
    return jq.jq(resultscript).transform(pointerize(n.result,False,n.identifier), multiple_output = True)

def process_wflowref(leafobj,view):
    nodeselector, resultscript = leafobj['$wflowref']
    nodestruct = jq.jq(nodeselector).transform(view.steps, multiple_output = True)
    return process_jsonlike(nodestruct, 'has("_nodeid")', lambda x: process_noderef(x, resultscript, view))

def process_wflowpointer(leafobj):
    p = leafobj['$wflowpointer']
    return outputReference(p['step'],jsonpointer.JsonPointer(p['result']))

@scheduler('jq-stage')
def jq_stage(stage, spec):
    '''
    :param stage: common stage parent object
    :param spec: stage JSON-like spec

    :return: None
    '''
    binds = spec['bindings']
    binds = process_jsonlike(binds,'has("$wflowref")',lambda x: process_wflowref(x,stage.view))
    log.info('transforming binds: %s', binds)
    stagescript = spec['stepscript']
    singlesteps = jq.jq(stagescript).transform(binds,multiple_output = False)

    singlesteppars = map(
        lambda x: process_jsonlike(x, 'has("$wflowpointer")',process_wflowpointer),
        singlesteps
    )

    postscript = spec['postscript']
    for i, pars in enumerate(singlesteppars):
        singlename = '{}_{}'.format(stage.name, i)

        finalized, inputs = finalize_input(pars, stage.view)
        log.info('postscripting: %s',finalized)
        after_post = jq.jq(postscript).transform(finalized,multiple_output = False)
        after_post = stage.datamodel.create(after_post)
        log.info('finalized to: %s',after_post)
        addStepOrWorkflow(singlename, stage, after_post, inputs, spec)
    registerExpressions(stage, spec.get('register_values'))

@scheduler('init-stage')
def init_stage(stage, spec):
    '''
    :param stage: common stage parent object
    :param spec: stage JSON-like spec

    :return: None
    '''
    inputs = []
    if spec.get('inputs'):
        inputs = list(map(outputReference.fromJSON, spec['inputs']))
        log.debug('initializing scope from dependent tasks with inputs')
    else:
        log.debug('initializing scope from dependent tasks')

    depstates = stage.state_provider.init_states if stage.state_provider else []

    if stage.state_provider:
        step_state = stage.state_provider.new_state(stage.name,depstates, readonly = True)
    else:
        step_state = None

    init_spec = spec['step']
    task = packtivity_task(spec['nodename'] or stage.name, init_spec,
        state = step_state, parameters = spec['parameters'], inputs = inputs
    )
    stage.addStep(task)
    registerExpressions(stage, spec.get('register_values'))

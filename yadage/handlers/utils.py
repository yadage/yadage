import json
import re
import logging
import itertools

log = logging.getLogger(__name__)

def handler_decorator():
    """
    create a pair of handler storage dictionary
    and decorator to declare a function a handler
    """
    handlers = {}
    def decorator(name):
        def wrap(func):
            handlers[name] = func
        return wrap
    return handlers,decorator


# class JSONPointerFormatter(string.Formatter):
#     def get_value(self,key,args,kwargs):
#         return jsonpointer.resolve_pointer(args[0],key)
#
# PointerFormatter = JSONPointerFormatter()
            
def evaluate_parameters(parameters,context):
    """
    values of in context are converted to strings via json.dump,
    string parameters are interpolated, and finally reloaded as json
    """
    dumped_context = {k:json.dumps(v) for k,v in context.iteritems()}
    evaluated = {}

    

    for k,v in parameters.iteritems():
        eval_val = v.format(**dumped_context) if type(v)==unicode or type(v)==str else v

        
        try:
            evaluated[k] = json.loads(eval_val)
        except:
            evaluated[k] = eval_val
    return evaluated
    
def stage_results(stage):
    for step in stage['scheduled_steps']:
        result = step.result_of()
        yield step.identifier,result

def match_steps(stages):
    for stepid,result in itertools.chain(*[stage_results(stage) for stage in stages]):
        yield stepid,result
    
def regex_match_outputs(stages,regex_list):
    """
    A generator returning tuples of 
    (output, output reference)
    for outputs of steps that are part of the stages
    and match a regular expression.
    List-outputs are flattened and elements yielded separately
    """
    for stepid,result in itertools.chain(*[stage_results(stage) for stage in stages]):
        for regex in map(re.compile,regex_list):
            matching_outputkeys = filter(regex.match,result.keys())
            for outputkey in matching_outputkeys:
                try:
                    output = result[outputkey]                
                except KeyError:
                    log.exception('could not fine output %s in metadata %s',outputkey,result)
                    raise
                if type(output) is not list:
                    yield (stepid,outputkey,None)
                else:
                    for i in range(len(output)):
                        yield (stepid,outputkey,i)

def read_input(dag,step,reference):
    """
    reads in the value of the reference and notes
    that is has been used by this step
    """
    stepid,outputkey,index = reference
    output = dag.getNode(stepid).result_of()[outputkey]
    value = output if index is None else output[index] 
    step.used_input(reference)
    return value

def addTask(dag,task):
    """
    Adds the task to the DAG and sets the dependencies of it based on the used inputs
    """
    dependencies = [dag.getNode(k) for k in task.inputs.keys()]
    node = dag.addTask(task, nodename = task.name, depends_on = dependencies)
    return node
    
def addStep(stage,dag,task):
    if 'scheduled_steps' not in stage:
        stage['scheduled_steps'] = []
    node = addTask(dag,task)
    stage['scheduled_steps'] += [node]
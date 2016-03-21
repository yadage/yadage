import json
import re
import logging
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
    
def regex_match_outputs(stages,regex_list):
    """
    A generator returning tuples of 
    (step, outputkey, index)
    for outputs of steps that are part of the stages
    and match a regular expression.
    For single-value outputs the index is None
    """
    for x in [step for stage in stages for step in stage['scheduled_steps']]:
        result = x.result_of()
        for regex in [re.compile(pattern) for pattern in regex_list]:
            matching_outputkeys = [k for k in result.keys() if regex.match(k)]
        
            for outputkey in matching_outputkeys:
                try:
                    output = result[outputkey]                
                except KeyError:
                    log.exception('could not fine output %s in metadata %s',outputkey,result)
                
                if type(output) is not list:
                    yield (x,outputkey,None)
                else:
                    for i,y in enumerate(output):
                        yield (x,outputkey,i)
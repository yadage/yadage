def handler_decorator():
    handlers = {}
    def decorator(name):
        def wrap(func):
            handlers[name] = func
        return wrap
    return handlers,decorator
    
def evaluate_parameters(parameters,context):
    """
    values of in context are converted to strings via json.dump,
    parameters then interpolated, and finally reloaded as json
    """
    dumped_context = {k:json.dumps(v) for k,v in context.iteritems()}
    evaluated = {}
    for k,v in parameters.iteritems():
        eval_val = v.format(**dumped_context)
        try:
            evaluated[k] = json.loads(eval_val)
        except ValueError:
            evaluated[k] = eval_val
    return evaluated

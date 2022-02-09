def handler_decorator():
    handlers = {}

    def decorator(name):
        def wrap(func):
            handlers[name] = func
            return func

        return wrap

    return handlers, decorator

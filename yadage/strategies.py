from adage.pollingexec import yes_man

from .interactive import custom_decider, decide_step, interactive_deciders
from .utils import advance_coroutine
from .handlers.utils import handler_decorator


handlers, strategy = handler_decorator()

def get_strategy(name):
    return handlers[name]()

@strategy('interactive')
def interactive_strategy():
    extend, submit = interactive_deciders()
    return dict(
            extend_decider = extend,
            submit_decider = submit
    )

@strategy('askforsubmit')
def askforsubmit():
    extend_decider = yes_man()
    advance_coroutine(extend_decider)

    submit_decider = custom_decider(decide_step, idbased = False)()
    advance_coroutine(submit_decider)

    return dict(
            extend_decider = extend_decider,
            submit_decider = submit_decider
    )

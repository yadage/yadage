import logging
import os
from backend import yadage_task

def ydg_step(func):
    def sig(*args,**kwargs):
        instance = yadage_task(func)
        instance.set(*args,**kwargs)
        return instance
    func.s = sig
    return func

def build_command(step):
    proc_type =  step['step_spec']['process']['process-type']
    from yadage.handlers.process_handlers import handlers as proc_handlers
    handler = proc_handlers[proc_type]
    command = handler(step['step_spec']['process'],step['attributes'])
    return command

@ydg_step
def yadagestep(step,global_context):
    steplog = '{}/{}.step.log'.format(os.path.abspath(global_context['workdir']),step['name'])
    log = logging.getLogger('step_logger_{}'.format(step['name']))

    fh  = logging.FileHandler(steplog)
    fh.setLevel(logging.DEBUG)
    log.addHandler(fh)
    
    log.info('starting log for step: %s'.format(step['name']))
    
    command = build_command(step)
    
    environment = step['step_spec']['environment']
    from yadage.handlers.environment_handlers import handlers as env_handlers
    handlercls = env_handlers[environment['environment-type']]
    handler = handlercls(step['name'],global_context,command,environment, log = log)
    handler.handle()


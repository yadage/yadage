import os
import logging

def publish(step,context):
    pubtype =  step['step_spec']['publisher']['publisher-type']
    from yadage.handlers.publisher_handlers import handlers as pub_handlers
    publisher = pub_handlers[pubtype]
    return publisher(step,context)

def build_command(step):
    proc_type =  step['step_spec']['process']['process-type']
    from yadage.handlers.process_handlers import handlers as proc_handlers
    handler = proc_handlers[proc_type]
    command = handler(step['step_spec']['process'],step['attributes'])
    return command

def run_in_env(environment,cmd,context,log,nametag):
    from yadage.handlers.environment_handlers import handlers as env_handlers
    handlercls = env_handlers[environment['environment-type']]
    handler = handlercls(nametag,log)
    return handler(environment,context,cmd)

def runstep(step,global_context):
    steplog = '{}/{}.step.log'.format(os.path.abspath(global_context['workdir']),step['name'])
    log = logging.getLogger('step_logger_{}'.format(step['name']))

    fh  = logging.FileHandler(steplog)
    fh.setLevel(logging.DEBUG)
    log.addHandler(fh)
    
    log.info('starting log for step: %s'.format(step['name']))
    
    command = build_command(step)

    environment = step['step_spec']['environment']
    runresult   = run_in_env(environment,command,global_context,log,step['name'])
    output      = publish(step,global_context)
    return output

class step(object):
    def __init__(self,name,spec,context):
        self.step_info = {}
        self.step_info['name'] = name
        self.step_info['step_spec'] = spec
        self.context = context
    def __call__(self,**attributes):
        self.step_info['attributes'] = attributes
        return runstep(self.step_info,global_context = self.context)

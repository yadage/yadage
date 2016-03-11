import os
import logging

def publish(step,context):
    pubtype =  step['step_spec']['publisher']['publisher-type']
    from yadage.handlers.publisher_handlers import handlers as pub_handlers
    publisher = pub_handlers[pubtype]
    return publisher(step,context)

def build_command(process,attributes):
    proc_type =  process['process-type']
    from yadage.handlers.process_handlers import handlers as proc_handlers
    handler = proc_handlers[proc_type]
    command = handler(process,attributes)
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

    log.debug('starting log for step: %s',step['name'])

    command = build_command(step['step_spec']['process'],step['attributes'])

    environment = step['step_spec']['environment']
    run_in_env(environment,command,global_context,log,step['name'])
    output      = publish(step,global_context)
    return output

class yadagestep(object):
    def __init__(self,name,spec,context):
        self.step_info = {}
        self.step_info['name'] = name
        self.step_info['step_spec'] = spec
        self.step_info['attributes'] = {}
        self.step_info['used_inputs'] = {}
        self.context = context

    def __repr__(self):
        return '<yadagestep name: {}>'.format(self.name)

    @property
    def step(self):
        return self.step_info

    @property
    def name(self):
        return self.step_info['name']
    
    @property
    def inputs(self):
        return self.step_info['used_inputs']
    
    def attr(self,key,value):
        self.step_info['attributes'][key] = value
    def s(self,**attributes):
        self.step_info['attributes'] = attributes
        return self
    def used_input(self,step,output,index):
        if not step in self.step_info['used_inputs']:
            self.step_info['used_inputs'][step] = []
        self.step_info['used_inputs'][step].append((output,index))
        
    def __call__(self,**attributes):
        self.step_info['attributes'].update(**attributes)
        return runstep(self.step_info,global_context = self.context)

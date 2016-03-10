import yaml
import utils

handlers, publisher = utils.handler_decorator()

@publisher('process-attr-pub')
def process_attr_pub_handler(step,context):
    outputs = {}
    for k,v in step['step_spec']['publisher']['outputmap'].iteritems():
      outputs[k] = [step['attributes'][v]]
    return outputs
    
@publisher('fromyaml-pub')
def fromyaml_pub_handler(step,context):
    yamlfile =  step['step_spec']['publisher']['yamlfile']
    yamlfile =  yamlfile.replace('/workdir',context['workdir'])
    pubdata = yaml.load(open(yamlfile))
    return pubdata
    
@publisher('dummy-pub')
def dummy_pub_handler(step,context):
    return  step['step_spec']['publisher']['publish']

import yadageschemas

def workflow(source, toplevel, schema_name='yadage/workflow-schema', schemadir=None, validate=True):
    '''
    load (and validate) workflow from source
    param source: URI fragment to the source
    param toplevel: base URI to resolve references from
    schemadir
    '''
    data = yadageschemas.load(source, toplevel, schema_name, schemadir, validate)
    return data

def validate(data, schema_name='yadage/workflow-schema', schemadir=None):
    '''validate workflow data'''
    yadageschemas.validator(schema_name,schemadir).validate(data)

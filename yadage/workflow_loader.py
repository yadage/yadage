import yadageschemas

def workflow(source, toplevel, schema_name='yadage/workflow-schema', schemadir=None, validate=True):
    data = yadageschemas.load(source, toplevel, schema_name, schemadir, validate)
    return data

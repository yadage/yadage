import capschemas

def workflow(source, toplevel, schema_name = 'yadage/workflow-schema', schemadir = None, validate = True):
    data = capschemas.load(source,toplevel,schema_name,schemadir,validate)
    return data
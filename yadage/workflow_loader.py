import capschemas

def workflow(source, toplevel, schema_name = '', schemadir = None, validate = True):
    data = capschemas.load(source,toplevel,'yadage/workflow-schema',schemadir,validate)
    return data
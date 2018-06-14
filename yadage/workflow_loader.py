import yadageschemas


def workflow(source, toplevel, schema_name='yadage/workflow-schema', schemadir=None, validate=True):
    '''
    load (and validate) workflow from source
    param source: URI fragment to the source
    param toplevel: base URI to resolve references from
    schemadir
    '''
    dialect, spec = ('raw_with_defaults', source) if ':' not in source else source.split(':',1)

    specopts = {
        'toplevel': toplevel,
        'schema_name': schema_name,
        'schemadir': schemadir,
        'load_as_ref': False
    }
    validopts = {
        'schema_name': schema_name,
        'schemadir': schemadir,
    }

    data = yadageschemas.load(
        spec, specopts, validate = True, validopts = validopts, dialect = dialect
    )
    return data

def validate(data, schema_name='yadage/workflow-schema', schemadir=None):
    '''validate workflow data'''
    validopts = {
        'schema_name': schema_name,
        'schemadir': schemadir,
    }
    yadageschemas.validate_spec(data,validopts)

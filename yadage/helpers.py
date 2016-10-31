import hashlib
import uuid
import json
import os
from backends.staticbackend import StaticProxy

def set_backend(dag,backend,proxymaker):
    '''
    sets backend and proxies for each node in the DAG.
    proxymaker is a 1-ary function that takes the node object and
    returns a suitable result proxy
    '''
    for nodename in dag.nodes():
        n = dag.getNode(nodename)
        n.backend = backend
        n.resultproxy = proxymaker(n)

def set_static_backend(dag,backend):
    '''
    sets the backend for the case of a static backend
    Proxies are set using the node identifier
    '''
    set_backend(
        dag,
        backend,
        proxymaker = lambda n: StaticProxy(n.identifier)
    )

def json_hash(jsonable):
    return hashlib.sha1(json.dumps(jsonable, cls = WithJsonRefEncoder, sort_keys = True)).hexdigest()

def get_id_fromjson(jsonobject, method = 'uuid'):
    method = os.environ.get('YADAGE_ID_METHOD',method)
    if method == 'uuid':
        return str(uuid.uuid1())
    elif method == 'jsonhash':
        return json_hash(jsonobject)
    else:
        raise NotImplementedError('unkown id generation method {}'.format(method))


def get_obj_id(obj_with_json_method, method = 'uuid'):
    return get_id_fromjson(obj_with_json_method.json(),method)


import jsonref
class WithJsonRefEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, jsonref.JsonRef):
            return {k:v for k,v in obj.iteritems()}
        else:
            super(WithJsonRefEncoder,self).default(obj)

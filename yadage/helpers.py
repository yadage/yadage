import hashlib
import uuid
import json
import os
import jsonref
from backends.trivialbackend import TrivialProxy, TrivialBackend
import logging
log = logging.getLogger(__name__)

def set_backend(dag, backend, proxymaker):
    '''
    sets backend and proxies for each node in the DAG.
    proxymaker is a 1-ary function that takes the node object and
    returns a suitable result proxy
    '''
    for nodename in dag.nodes():
        n = dag.getNode(nodename)
        n.backend = backend
        n.resultproxy = proxymaker(n)


def set_trivial_backend(dag, jsondata):
    '''
    sets the backend for the case of a static backend
    Proxies are set using the node identifier
    '''
    backend = TrivialBackend()
    set_backend(
        dag,
        backend,
        proxymaker=lambda n: TrivialProxy(
            status=jsondata[n.identifier]['status'],
            result=jsondata[n.identifier]['result']
        )
    )
    return backend


def stepindex(wflow):
    ''''
    build an index mapping step id's to rule id's
    '''
    index = {}
    for r in wflow.applied_rules + wflow.rules:
        for s in wflow.view(r.offset).getSteps(r.rule.name):
            index[s.identifier] = r.identifier
    return index

DEFAULT_ID_METHOD = 'jsonhash'


def json_hash(jsonable):
    # log.info('hashing: %s',json.dumps(jsonable, cls=WithJsonRefEncoder, sort_keys=True))
    hash  = hashlib.sha1(json.dumps(jsonable, cls=WithJsonRefEncoder, sort_keys=True)).hexdigest()
    # log.info('got %s',hash)
    return hash


def get_id_fromjson(jsonobject, method = DEFAULT_ID_METHOD):
    method = os.environ.get('YADAGE_ID_METHOD', method)
    if method == 'uuid':
        return str(uuid.uuid1())
    elif method == 'jsonhash':
        return json_hash(jsonobject)
    else:
        raise NotImplementedError(
            'unkown id generation method {}'.format(method))

def get_obj_id(obj_with_json_method, method = DEFAULT_ID_METHOD):
    return get_id_fromjson(obj_with_json_method.json(), method)


class WithJsonRefEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, jsonref.JsonRef):
            return {k: v for k, v in obj.iteritems()}
        else:
            super(WithJsonRefEncoder, self).default(obj)

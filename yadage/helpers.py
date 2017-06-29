import hashlib
import uuid
import json
import os
import jsonref
import copy
from backends.trivialbackend import TrivialProxy, TrivialBackend
from yadagestep import outputReference

import logging
import jsonpointer

log = logging.getLogger(__name__)

def leaf_iterator(jsonlike, path = None):
    ''''
    :param jsonlike: a jsonlike object (i.e. nested lists/arrays: leaf-types must not be JSONable)
    :return: iterator that yields (JSON path, leaf value) tuples
    '''
    path = path or []
    if type(jsonlike) == list:
        for i,x in enumerate(jsonlike):
            thispath = copy.deepcopy(path)
            thispath.append(i)
            for leaf in leaf_iterator(x, path = thispath):
                yield leaf
    elif type(jsonlike) == dict:
        for k,v in jsonlike.iteritems():
            thispath = copy.deepcopy(path)
            thispath.append(k)
            for leaf in leaf_iterator(v, path = thispath):
                yield leaf
    else:
        yield jsonpointer.JsonPointer.from_parts(path),jsonlike

def process_refs(x, dag):
    if type(x) == dict:
        for k, v in x.iteritems():
            x[k] = process_refs(v, dag)
        return x
    elif type(x) == list:
        for i, e in enumerate(x):
            x[i] = process_refs(e, dag)
        return x
    elif type(x) == outputReference:
        return x.pointer.resolve(dag.getNode(x.stepid).result)
    else:
        return x

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


DEFAULT_ID_METHOD = 'jsonhash'


def json_hash(jsonable):
    # log.info('hashing: %s',json.dumps(jsonable, cls=WithJsonRefEncoder, sort_keys=True))
    the_hash  = hashlib.sha1(json.dumps(jsonable, cls=WithJsonRefEncoder, sort_keys=True)).hexdigest()
    # log.info('got %s',hash)
    return the_hash


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
        try:
            super(WithJsonRefEncoder, self).default(obj)
        except TypeError:
            return obj.json()
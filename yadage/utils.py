import json
import yaml
import os
import click
import zipfile
import logging
import jsonpointer
import jq
import hashlib
import uuid
import copy
import importlib

from yadageschemas.utils import WithJsonRefEncoder

class outputReference(object):

    def __init__(self, stepid, pointer):
        self.stepid = stepid
        self.pointer = pointer

    def __repr__(self):
        return 'outputReference {}#{}'.format(self.stepid, self.pointer.path)

    #(de-)serialization
    @classmethod
    def fromJSON(cls, data):
        return cls(data['stepid'], jsonpointer.JsonPointer(data['pointer_path']))

    def json(self):
        return {
            'stepid': self.stepid,
            'pointer_path': self.pointer.path
        }

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

log = logging.getLogger(__name__)

def set_backend(dag, backend, proxymaker):
    '''
    sets backend and proxies for each node in the DAG.
    proxymaker is a 1-ary function that takes the node object and
    returns a suitable result proxy

    :param dag: the dag object (par of the workflow object)
    :param backend: the backend to associate with the nodes
    :param proxymaker: function object with signature f(node) to create result proxies
    '''
    for nodename in dag.nodes():
        n = dag.getNode(nodename)
        n.backend = backend
        n.resultproxy = proxymaker(n)

DEFAULT_ID_METHOD = 'jsonhash'


def json_hash(jsonable):
    '''
    returns a content-based hash of a json-able structure (serialized with WithJsonRefEncoder)

    :param jsonable: a json-serializable object
    :return: the hash
    '''
    the_hash  = hashlib.sha1(json.dumps(jsonable, cls=WithJsonRefEncoder, sort_keys=True).encode('utf-8')).hexdigest()
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

def process_refs(x, dag):
    if type(x) == dict:
        for k, v in x.items():
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

def leaf_iterator(jsonable):
    '''
    generator function to yield leafs items of a JSON-like structure alongside
    their position in the structure as determined by a JSONPointer.

    :param jsonable: a json-serializable object
    :return: tuples (jsonpointer, leaf value)
    '''
    allleafs = jq.jq('leaf_paths').transform(jsonable, multiple_output = True)
    leafpointers = [jsonpointer.JsonPointer.from_parts(x) for x in allleafs]
    for x in leafpointers:
        yield x,x.get(jsonable)

def leaf_iterator_jsonlike(jsonlike, path = None):
    ''''
    :param jsonlike: a jsonlike object (i.e. nested lists/arrays: leaf-types must not be JSONable)
    :return: iterator that yields (JSON path, leaf value) tuples
    '''
    path = path or []
    if type(jsonlike) == list:
        for i,x in enumerate(jsonlike):
            thispath = copy.deepcopy(path)
            thispath.append(i)
            for leaf in leaf_iterator_jsonlike(x, path = thispath):
                yield leaf
    elif type(jsonlike) == dict:
        for k,v in jsonlike.items():
            thispath = copy.deepcopy(path)
            thispath.append(k)
            for leaf in leaf_iterator_jsonlike(v, path = thispath):
                yield leaf
    else:
        yield jsonpointer.JsonPointer.from_parts(path),jsonlike


def process_jsonlike(jsonlike, jq_obj_selector, callback):
    wflowrefs = [jsonpointer.JsonPointer.from_parts(x) for x in jq.jq(
            'paths(if objects then {} else false end)'.format(jq_obj_selector)
            ).transform( jsonlike, multiple_output = True
    )]
    for wflowref in wflowrefs:
        value = callback(wflowref.resolve(jsonlike))
        wflowref.set(jsonlike,value)
    return jsonlike

def pointerize(jsondata, asref=False, stepid=None):
    '''
    a helper method that replaces leaf nodes in a JSON object with
    a outputReference objects (~ a JSONPath) pointing to that leaf position
    useful to track access to leaf nodes later on.
    '''
    allleafs = jq.jq('leaf_paths').transform(jsondata, multiple_output=True)
    leafpointers = [jsonpointer.JsonPointer.from_parts(x).path for x in allleafs]
    jsondata_proxy = copy.deepcopy(jsondata)
    for leaf in leafpointers:
        x = jsonpointer.JsonPointer(leaf)
        x.set(jsondata_proxy, outputReference(stepid, x) if asref else {'$wflowpointer': {'step': stepid,'result': x.path}} if stepid else x.path)
    return jsondata_proxy

def options_from_eqdelimstring(opts):
    options = {}
    for x in opts:
        key, value = x.split('=')
        options[key] = yaml.load(value)
    return options

def getinit_data(initfiles, parameters):
    '''
    get initial data from both a list of files and a list of 'pname=pvalue'
    strings as they are passed in the command line <pvalue> is assumed to be a
    YAML parsable string.
    '''

    initdata = {}
    for initfile in initfiles:
        log.info('loading initialization data from file %s',initfile)
        initdata.update(**yaml.load(open(initfile)))

    initdata.update(**options_from_eqdelimstring(parameters))
    return initdata

def prepare_workdir_from_archive(initdir, inputarchive):
    if os.path.exists(initdir):
        raise click.exceptions.ClickException(click.style(
            "initialization directory exists and input archive give. Can't have both", fg='red'))
    os.makedirs(initdir)
    localzipfile = '{}/.yadage_inputarchive.zip'.format(initdir)
    f = urlopen(inputarchive)
    with open(localzipfile,'wb') as lf:
        lf.write(f.read())
    with zipfile.ZipFile(localzipfile) as zf:
        zf.extractall(path=initdir)
    os.remove(localzipfile)

def setupbackend_fromstring(backend, backendopts = None):
    backendopts = backendopts or {}
    log.info('setting up backend %s with opts %s', backend, backendopts)
    import yadage.backends.packtivitybackend as pb
    return pb.PacktivityBackend(
            packtivity_backendstring = backend,
            backendopts = backendopts
    )

def rootprovider_from_string(dataarg,dataopts = None):
    log.info('%s %s',dataarg,dataopts)
    if dataarg.startswith('local:'):
        import yadage.state_providers.localposix
        return yadage.state_providers.localposix.setup_provider(dataarg,dataopts)
    if dataarg.startswith('py:'):
        _,module, setupfunc,dataarg = dataarg.split(':',3)
        module = importlib.import_module(module)
        setupfunc = getattr(module,setupfunc)
        return setupfunc(dataarg,dataopts)
    if dataarg.startswith('fromenv:'):
        module = importlib.import_module(os.environ['PACKTIVITY_STATEPROVIDER'])
        return module.setup_provider(dataarg,dataopts)
    raise RuntimeError('unknown data type %s %s', dataarg, dataopts)

def get_init_spec(discover):
    return {
        'process': None,
        'environment': None,
        'publisher': {
            'publisher_type': 'fromparjq-pub',
            'script': '.',
            'tryExact': True,
            'glob': discover,
            'relative_paths': True
        }
    }

def init_stage_spec(parameters, discover, used_inputs, name, nodename = None):
    return {
        'name': name,
        'dependencies': {
            "dependency_type": "jsonpath_ready",
            "expressions": []
        },
        'scheduler': {
             'scheduler_type': 'init-stage',
             'parameters': parameters,
             'inputs':   used_inputs,
             'nodename': nodename,
             'discover': discover
        }
    }

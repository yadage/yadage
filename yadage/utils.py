import copy
import hashlib
import json
import logging
import os
import uuid

import jq
import jsonpointer
import yaml

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


log = logging.getLogger(__name__)

DEFAULT_ID_METHOD = 'uuid'


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
        return str(uuid.uuid4())
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
    wflowrefs = [jsonpointer.JsonPointer.from_parts(x[1:]) for x in jq.jq(
            'paths(if objects then {} else false end)'.format(jq_obj_selector)
            ).transform({'value': jsonlike}, multiple_output = True
    )]
    for wflowref in wflowrefs:
        value = callback(wflowref.resolve(jsonlike))
        if wflowref.path == '':
            return value
        else:
            wflowref.set(jsonlike,value)
    return jsonlike


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
        log.debug('loading initialization data from file %s',initfile)
        initdata.update(**yaml.load(open(initfile)))

    initdata.update(**options_from_eqdelimstring(parameters))
    return initdata

def setupbackend_fromstring(backend, backendopts = None):
    backendopts = backendopts or {}
    log.debug('setting up backend %s with opts %s', backend, backendopts)
    import yadage.backends.packtivitybackend as pb
    return pb.PacktivityBackend(
            packtivity_backendstring = backend,
            backendopts = backendopts
    )

def init_stage_spec(parameters, discover, used_inputs, name, nodename = None, relative = False):
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
             'step': {
                'process': None,
                'environment': None,
                'publisher': {
                    'publisher_type': 'fromparjq-pub',
                    'script': '.',
                    'tryExact': True,
                    'glob': discover,
                    'relative_paths': relative
                }
            }
        }
    }

def stages_in_scope(workflow,scope):
    return jsonpointer.JsonPointer(scope).resolve(workflow.bookkeeping)['_meta']['stages']

def rule_steps_indices(workflow):
    rule_to_steps_index = {}
    steps_to_rule_index = {}
    rule_to_subscopes_index = {}
    for rule in workflow.rules + workflow.applied_rules:
        path = '/'.join([rule.offset, rule.rule.name])
        p = jsonpointer.JsonPointer(path)
        try:
            steps_of_rule = [x['_nodeid'] for x in p.resolve(workflow.stepsbystage) if '_nodeid' in x]
        except jsonpointer.JsonPointerException:
            steps_of_rule = []

        try:
            a = p.resolve(workflow.stepsbystage)
            subscopes_of_rule = [
                # ['{}/{}'.format(x['_offset'],substage) for substage in x.keys() if not substage== '_offset' ]
                x['_offset']
                for x in a if '_offset' in x
            ]
        except jsonpointer.JsonPointerException:
            subscopes_of_rule = []

        rule_to_steps_index[rule.identifier] = steps_of_rule
        rule_to_subscopes_index[rule.identifier] = subscopes_of_rule
        for step in steps_of_rule:
            steps_to_rule_index[step] = rule.identifier
    return rule_to_steps_index, steps_to_rule_index, rule_to_subscopes_index

def advance_coroutine(coroutine):
    try:
        return coroutine.next()
    except AttributeError:
        return coroutine.__next__()

def prepare_meta(metadir, accept=False):
    '''
    prepare workflow meta-data directory

    :param metadir: the meta-data directory name
    :param accept: whether to accept an existing metadata directory
    '''
    if os.path.exists(metadir):
        if not accept:
            raise RuntimeError("yadage meta directory %s exists. Allow overwrite by using the command line option  --accept-metadir" % metadir)
    else:
        os.makedirs(metadir)

def pointerize(data, asref=False, stepid=None):
    def callback(p):
        return outputReference(stepid, p) if asref else {'$wflowpointer': {'step': stepid,'result': p.path}} if stepid else p.path
    return data.asrefs(callback = callback)

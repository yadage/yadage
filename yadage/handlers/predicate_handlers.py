import logging
import utils
import jsonpointer
import jsonpath_rw

log = logging.getLogger(__name__)

handlers,scheduler = utils.handler_decorator()

def convert(thing):
    if type(thing)==jsonpath_rw.jsonpath.Index:
        return thing.index
    if type(thing)==jsonpath_rw.jsonpath.Fields:
        fs = thing.fields
        assert len(fs)==1
        return fs[0]

def unravelpath(path):
    if type(path)==jsonpath_rw.jsonpath.Child:
        for x in unravelpath(path.left):
            yield x
        yield convert(path.right)
    else:
        yield convert(path)

def path2pointer(path):
    return jsonpointer.JsonPointer.from_parts(x for x in unravelpath(path)).path

def checkmeta(flowview,metainfo):
    applied_ids = [rl.identifier for rl in flowview.applied_rules]
    rulesok = all([x in applied_ids for x in metainfo['rules']])
    stepsok = all([flowview.dag.getNode(x).has_result for x in metainfo['steps']])
    return (rulesok and stepsok)


def scope_done(scope,flowview):
    result = True
    
    bookkeeper = jsonpointer.JsonPointer(scope).resolve(flowview.bookkeeper)
    for k,v in bookkeeper.iteritems():
        for k,v in bookkeeper.iteritems():
            if k=='_meta':
                result = result and checkmeta(flowview,v)
            else:
                childscope = scope+'/{}'.format(k)
                result = result and scope_done(childscope,flowview)
    return result

@scheduler('jsonpath_ready')
def jsonpath_ready(flowview,depspec):
    dependencies = depspec['expressions']
    for x in dependencies:
        depmatches = flowview.query(x,flowview.steps)
        
        if not depmatches:
            return False
        issubwork = '_nodeid' not in depmatches[0].value[0]
        if issubwork:
            if not all([scope_done(scope['_offset'],flowview) for match in depmatches for scope in match.value]):
                return False
        else:
            if not all([x.has_result() for x in flowview.getSteps(x)]):
                return False
    return True
from packtivity.typedleafs import TypedLeafs
from .utils import outputReference

def data_from_json(jsondata, datamodel = None):
    return TypedLeafs(jsondata,datamodel)


def pointerize(typedleafs, asref=False, stepid=None):
    '''
    a helper method that replaces leaf nodes in a JSON object with
    a outputReference objects (~ a JSONPath) pointing to that leaf position
    useful to track access to leaf nodes later on.
    '''
    pointerized = typedleafs.copy().json()
    for p,v in typedleafs.leafs():
        newval = outputReference(stepid, p) if asref else {'$wflowpointer': {'step': stepid,'result': p.path}} if stepid else p.path
        if p.path=='': return newval #there is only one root leaf
        p.set(pointerized, newval)
    return pointerized

def leafs(data):
    for p,v in data.leafs():
        yield p,v
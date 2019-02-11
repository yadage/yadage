from packtivity import datamodel as pdm
from .utils import outputReference

def data_from_json(jsondata, datamodel = None):
    return pdm.create(jsondata,datamodel)

def pointerize(typedleafs, asref=False, stepid=None):
    def callback(p):
        return outputReference(stepid, p) if asref else {'$wflowpointer': {'step': stepid,'result': p.path}} if stepid else p.path
    return typedleafs.asrefs(callback = callback)


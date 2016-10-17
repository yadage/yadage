import logging
import utils
import jq
import jsonpointer
import copy
from yadage.yadagestep import outputReference
import jsonpath_rw

log = logging.getLogger(__name__)

handlers,scheduler = utils.handler_decorator()

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
        x.set(jsondata_proxy, outputReference(stepid, x) if asref else x.path)
    return jsondata_proxy


def select_reference(step, selection):
    '''
    resolves a jsonpath selection and returns JSONPointerized matches
    '''
    log.debug('selecting output from step %s',step)
    pointerized = pointerize(step.result, asref=True, stepid=step.identifier)
    matches = jsonpath_rw.parse(selection).find(pointerized)
    if not matches:
        log.error('no matches found for selection %s in result %s', selection, step.result)
        raise RuntimeError('no matches found in reference selection')

    if len(matches) > 1:
        log.error('found multiple matches to query: %s within result: %s\n \ matches %s', selection, step.result, matches)
        raise RuntimeError('multiple matches in result jsonpath query')
    return matches[0].value


def combine_outputs(outputs, flatten, unwrapsingle):
    '''
    combines the result of multiple reference selections into a single outputs.
    non-list values will just be combined into a list while lists will be concatenated
    optinally we can return the sole element of a single-value list (argument: unwrapsingle)
    '''
    combined = []
    for reference in outputs:
        if type(reference)==list:
            if flatten:
                for elementref in reference:
                    combined+=[elementref]
            else:
                combined+=[reference]
        else:
            combined+=[reference]
    if len(combined)==1 and unwrapsingle:
        combined = combined[0]
    return combined

def select_steps(stageview,query):
    return stageview.getSteps(query)

def select_outputs(steps,selection,flatten,unwrapsingle):
    return combine_outputs(map(lambda s: select_reference(s, selection), steps), flatten, unwrapsingle)

@scheduler('stage-output-selector')
def resolve_reference(stageview,selection):
    '''resolves a output reference by selecting the stage and stage outputs'''
    log.debug('resolving selection %s',selection)
    if type(selection) is not dict:
        return None
    else:
        steps = select_steps(stageview, selection['stages'])
        log.debug('selected steps %s',steps)
        outputs = select_outputs(steps,
                                 selection['output'],
                                 selection.get('flatten', False),
                                 selection.get('unwrap', False))
        log.debug('selected outputs %s',outputs)
        return outputs

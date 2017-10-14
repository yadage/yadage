import logging
import yadage.handlers.utils as utils
import jsonpath_rw
from ..utils import pointerize

log = logging.getLogger(__name__)

handlers, scheduler = utils.handler_decorator()



def select_reference(step, selection):
    '''
    resolves a jsonpath selection on a step's result data and returns JSONPointerized matches

    :param step: the step holding the result data
    :param selection: a JSONPath expression

    :return: the first (and single) match of the expression as a JSON pointer
    '''
    log.debug('selecting output from step %s', step)
    pointerized = pointerize(step['result'], asref=True, stepid=step['id'])
    matches = jsonpath_rw.parse(selection).find(pointerized)
    if not matches:
        log.error('no matches found for selection %s in result %s',
                  selection, step['result'])
        raise RuntimeError('no matches found in reference selection. selection %s | result %s', selection, step['result'])

    if len(matches) > 1:
        log.error('found multiple matches to query: %s within result: %s\n \ matches %s',
                  selection, step['result'], matches)
        raise RuntimeError('multiple matches in result jsonpath query')
    return matches[0].value

def combine_outputs(outputs, flatten, unwrapsingle):
    '''
    combines the result of multiple reference selections into a single outputs.
    non-list values will just be combined into a list while lists will be concatenated
    optinally we can return the sole element of a single-value list (argument: unwrapsingle)

    :param outputs:

    '''
    combined = []
    for reference in outputs:
        if type(reference) == list:
            if flatten:
                for elementref in reference:
                    combined += [elementref]
            else:
                combined += [reference]
        else:
            combined += [reference]
    if len(combined) == 1 and unwrapsingle:
        combined = combined[0]
    return combined


def select_steps(stageview, query):
    '''
    selects the step objects from the stage view based on a query and converts them to simple
    dictionaries with id,result keys

    :param stageview: the view object on which to perform the query
    :param query: a slection query (JSONPath expression)

    :return: list of {id: xx, result: yy} dictionaries
    '''
    return [{'id': s.identifier, 'result': s.result} for s in stageview.getSteps(query)]


def select_outputs(steps, selection, flatten, unwrapsingle):
    return combine_outputs(map(lambda s: select_reference(s, selection), steps), flatten, unwrapsingle)


@scheduler('stage-output-selector')
def resolve_reference(stageview, selection):
    '''
    :param stageview: the workflow view objct
    :param selection: the JSON-like selection dictionary
    :return :
    resolves a output reference by selecting the stage and stage outputs
    '''
    log.debug('resolving selection %s', selection)
    if type(selection) is not dict:
        return None
    else:
        steps = select_steps(stageview, selection['stages'])
        log.debug('selected steps %s', steps)
        outputs = select_outputs(steps,
                                 selection['output'],
                                 selection.get('flatten', False),
                                 selection.get('unwrap', False))
        log.debug('selected outputs %s', outputs)
        return outputs



@scheduler('jq-stage-output-selector')
def resolve_jq_reference(stageview, selection):
    return

import logging
import jsonpath_rw

from .utils import handler_decorator
from ..utils import pointerize

log = logging.getLogger(__name__)

handlers, expression = handler_decorator()



def select_reference(step, selection):
    '''
    resolves a jsonpath selection on a step's result data and returns JSONPointerized matches

    :param step: the step holding the result data
    :param selection: a JSONPath expression

    :return: the first (and single) match of the expression as a JSON pointer
    '''
    selection = selection or '$'
    log.debug('selecting output from step %s', step)
    pointerized = pointerize(step['result'], asref=True, stepid=step['id'])
    matches = jsonpath_rw.parse(selection).find(pointerized)
    if not matches:
        log.error('no matches found for selection %s in result %s' %
                  (selection, step['result']))
        raise RuntimeError('no matches found in reference selection. selection %s | result %s' % (selection, step['result']))

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
    log.debug('flatten %s unwrap: %s', flatten, unwrapsingle)
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

@expression('stage-output-selector')
def stage_output_selector(stageview, selection):
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
        if 'stages' in selection or 'steps' in selection:
            if 'stages' in selection and 'steps' in selection:
                raise RuntimeError('stages and steps are aliases. pick one.')
            steps_selector = selection.get('stages') or selection.get('steps')
            steps = select_steps(stageview, steps_selector)
            log.debug('selected steps %s %s', len(steps), steps)
            outputs = select_outputs(steps,
                                     selection.get('output'),
                                     selection.get('flatten', False),
                                     selection.get('unwrap', False))
            log.debug('selected outputs %s', outputs)
            return outputs
        elif 'step' in selection:
            steps = select_steps(stageview, selection['step'])
            assert len(steps) == 1
            step = steps[0]
            return select_reference(step, selection.get('output'))
        raise RuntimeError('not sure how to deal with this.')

@expression('fromvalue')
def value_resolver(view, expression):
    value = view.getValue(expression['key'])
    if not value:
        return None
    log.debug('resolved to %s', value)
    if isinstance(value,dict) and 'expression_type' in value:
        log.debug('looking up expression %s', value)
        return handlers[value['expression_type']](view, value)
    else:
        log.info('not an expression value %s', value)
        return value

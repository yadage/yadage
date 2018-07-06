import logging

import jsonpointer

import yadage.handlers.utils as utils
from yadage.handlers.expression_handlers import handlers as exprhandlers

log = logging.getLogger(__name__)

handlers, predicate = utils.handler_decorator()


def checkmeta(flowview, metainfo):
    log.debug('checking meta %s on view with offset %s',
              metainfo, flowview.offset)
    applied_ids = [rl.identifier for rl in flowview.applied_rules]
    rulesok = all([x in applied_ids for x in metainfo['stages']])

    stepsok = all([flowview.dag.getNode(x).has_result()
                   for x in metainfo['steps']])
    log.debug('all rules applied: %s, all steps have results: %s',
              rulesok, stepsok)
    return (rulesok and stepsok)


def scope_done(scope, flowview):
    '''
    walks recursively all scopes starting at some initial scope to determine if
    all steps and stages under this scope have been executed / applied. Will indicate
    that it's safe to reference any result of the workflow within that scope.
    '''
    log.debug('checking scope %s on view with offset %s',
              scope, flowview.offset)
    result = True

    bookkeeper = jsonpointer.JsonPointer(scope).resolve(flowview.bookkeeper)
    for k, v in bookkeeper.items():
        for k, v in bookkeeper.items():
            if k == '_meta':
                result = result and checkmeta(flowview, v)
            else:
                childscope = scope + '/{}'.format(k)
                result = result and scope_done(childscope, flowview)
    return result

@predicate('jsonpath_ready')
def jsonpath_ready(stage, depspec,stagespec):
    '''
    the main predicate for yadage. for a list of jsonpath expressions
    determine whether the stage or workflow scope is ready (i.e. has a result)
    '''
    log.debug('checking jsonpath ready predicate\n%s', depspec)
    dependencies = depspec['expressions']
    for x in dependencies:
        depmatches = stage.view.query(x, stage.view.steps)
        if not depmatches:
            log.debug('no query matches, not ready')
            return False
        issubwork = '_nodeid' not in depmatches[0].value[0]
        if issubwork:
            log.debug('dependency is a subworkflow. determine if scope is done')
            if not all([scope_done(scope['_offset'], stage.view) for match in depmatches for scope in match.value]):
                return False
        else:
            if not all([x.has_result() for x in stage.view.getSteps(x)]):
                return False
    log.debug('all checks ok, predicate is True')
    return True


@predicate('expressions_fulfilled')
def expressions_fulfilled(stage, depspec,stagespec):
    '''
    the main predicate for yadage. for a list of jsonpath expressions
    determine whether the stage or workflow scope is ready (i.e. has a result)
    '''
    log.debug('checking jsonpath ready predicate\n%s', depspec)
    expressions = depspec['expressions']
    for expression in expressions:
        handler = exprhandlers[expression['expression_type']]
        value = handler(stage.view, expression)
        if not value:
            return False
    return True

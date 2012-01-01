import logging
import utils
from yadage.yadagestep import yadagestep

log = logging.getLogger(__name__)

handlers,scheduler = utils.handler_decorator()

### A scheduler does the following things:
###   - attached new nodes to the DAG
###   - for each added step
###     - the step is given a name
###     - the step attributes are determined using the scheduler spec and context
###     - a list of used inputs (in the form of [stepname,outputkey,index])

@scheduler('simple-stage')
def simple_stage(stage,spec):
    print 'ok simple...'
    resolved_params = {k:resolve()}
    stage.stageinfo['parameters']
    [x.result[a['output']] for x in stage.flowview.getSteps(a['stages'])]


@scheduler('dummy')
def simple_stage(stage,spec):
    print stage.stageinfo

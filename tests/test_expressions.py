import yadage.handlers.expression_handlers as exh
import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
import packtivity.statecontexts.posixfs_context as statecontext
from yadage.helpers import process_refs

def test_stage_output_selector(nested_mapreduce_wflow):
    wflow = nested_mapreduce_wflow
    inputdata = {'here':[1,2,3],'deeply':{'nested':{'this':['h','e','r','e']}}}
    view = wflow.view()
    view.init({'input':inputdata})
    view.getRule(name = 'init').apply(wflow)

    result =  exh.handlers['stage-output-selector'](view,{'stages': 'init', 'output': 'input.here'})
    values = process_refs(result,wflow.dag)
    assert values == [inputdata['here']]

    result =  exh.handlers['stage-output-selector'](view,{'stages': 'init', 'output': 'input.here', 'unwrap': True})
    values = process_refs(result,wflow.dag)
    assert values == inputdata['here']

    result =  exh.handlers['stage-output-selector'](view,{'stages': 'init', 'output': 'input.deeply', 'unwrap': True})
    values = process_refs(result,wflow.dag)
    # assert values == inputdata['deeply']
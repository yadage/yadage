import yadage.handlers.expression_handlers as exh
import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
import packtivity.statecontexts.posixfs_context as statecontext
from yadage.helpers import process_refs

def test_multistepstage_schedule_wflows():
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/nestedmapreduce')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context('/workdir'))

    inputdata = [1,2,3]
    view = wflow.view()
    view.init({'input':inputdata})
    view.getRule(name = 'init').apply(wflow)

    result =  exh.handlers['stage-output-selector'](view,{'stages': 'init', 'output': 'input'})
    values = process_refs(result,wflow.dag)
    assert values == [inputdata]

    result =  exh.handlers['stage-output-selector'](view,{'stages': 'init', 'output': 'input', 'unwrap': True})
    values = process_refs(result,wflow.dag)
    assert values == inputdata
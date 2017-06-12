import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
from yadage.wflowview import offsetRule
import packtivity.statecontexts.posixfs_context as statecontext
def test_multistepstage_schedule_wflows():
    data  = yadage.workflow_loader.workflow('workflow.yml','tests/testspecs/nestedmapreduce')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context('/workdir'))


    inputdata = [1,2,3]
    wflow.view().init({'input':inputdata})
    assert wflow.view().getRule(name = 'map').applicable(wflow) == False

    wflow.view().getRule(name = 'init').apply(wflow)
    assert len(wflow.dag.nodes()) == 1 
    assert wflow.view().getRule(name = 'map').applicable(wflow) == True


    wflow.view().getRule(name = 'map').apply(wflow)
    assert wflow.view().getRule(name = 'reduce').applicable(wflow) == False
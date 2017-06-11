import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
import packtivity.statecontexts.posixfs_context as statecontext

def test_singlestepstage_schedule_steps():
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/local-helloworld')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context('/workdir'))
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stageinfo['scheduler_type'] == 'singlestep-stage'
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1


def test_multistepstage_schedule_wflows():
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/nestedmapreduce')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context('/workdir'))

    inputdata = [1,2,3]
    wflow.view().init({'input':inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stageinfo['scheduler_type'] == 'multistep-stage'
    assert len(wflow.view('').rules) == 3 # init map reduce
    wflow.rules[0].apply(wflow)
    assert len(wflow.view('/map/0').rules) == 3 + 2*len(inputdata) # 3x init and stage rules


def test_multistepstage_zip_schedule_steps():
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/mapreduce')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context('/workdir'))

    inputdata = [1,2,3]
    wflow.view().init({'input':inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stageinfo['scheduler_type'] == 'multistep-stage'
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 + len(inputdata)  

def test_multistepstage_cartesian_schedule_steps():
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/cartesian_mapreduce')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context('/workdir'))

    factor_one = [1,2,3]
    factor_two = [4,5]
    wflow.view().init({'factor_one':factor_one,'factor_two':factor_two})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stageinfo['scheduler_type'] == 'multistep-stage'
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 + len(factor_one)*len(factor_two)  


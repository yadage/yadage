import yadage.workflow_loader
from yadage.wflow import YadageWorkflow

def test_singlestepstage_schedule_steps(local_helloworld_wflow):
    wflow = local_helloworld_wflow
    assert wflow.rules[-1].applicable(wflow) == False
    assert wflow.rules[-1].rule.stageinfo['scheduler_type'] == 'singlestep-stage'
    wflow.rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1


def test_nested_wflow(nested_wflow):
    wflow = nested_wflow

    inputdata = 'world'
    wflow.view().init({'input':inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stageinfo['scheduler_type'] == 'singlestep-stage'
    assert len(wflow.view('').rules) == 3 # init nested higherscope
    wflow.rules[0].apply(wflow)
        # assert len(wflow.view('/nested/0').rules) == 3 + 2*len(inputdata) # 3x init and stage rules


def test_multistepstage_schedule_wflows(nested_mapreduce_wflow):
    wflow = nested_mapreduce_wflow

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

def test_jq_stage(jqworkflow):
    wflow = jqworkflow
    
    inputdata = [1,2,3]
    wflow.view().init({'parone':inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stageinfo['scheduler_type'] == 'jq-stage'
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 + len(inputdata)  


def test_multistepstage_zip_schedule_steps(simple_mapreduce):
    wflow = simple_mapreduce
    
    inputdata = [1,2,3]
    wflow.view().init({'input':inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stageinfo['scheduler_type'] == 'multistep-stage'
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 + len(inputdata)  

def test_multistepstage_cartesian_schedule_steps(cartesian_mapreduce):
    wflow = cartesian_mapreduce

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


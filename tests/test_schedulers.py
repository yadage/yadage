import yadage.workflow_loader
from yadage.wflow import YadageWorkflow

def test_singlestepstage_schedule_steps(local_helloworld_wflow):
    wflow = local_helloworld_wflow
    assert wflow.rules[-1].applicable(wflow) == False
    assert wflow.rules[-1].rule.stagespec['scheduler_type'] == 'singlestep-stage'
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
    assert wflow.rules[0].rule.stagespec['scheduler_type'] == 'singlestep-stage'
    assert len(wflow.view('').rules) == 3 # init nested higherscope
    wflow.rules[0].apply(wflow)

def test_multistepstage_schedule_wflows(nested_mapreduce_wflow):
    wflow = nested_mapreduce_wflow

    inputdata = [1,2,3]
    wflow.view().init({'input':inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stagespec['scheduler_type'] == 'multistep-stage'
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
    assert wflow.rules[0].rule.stagespec['scheduler_type'] == 'jq-stage'
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 + len(inputdata)  

def test_jqnodestruct_stage(jqnodestruct):
    wflow = jqnodestruct
    
    inputdata = {'scatterone': ['a','b','c'], 'scatertwo': [1,2,3]}

    wflow.view().init(inputdata)
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[1].applicable(wflow) == True
    # assert wflow.rules[0].rule.stagespec['scheduler_type'] == 'jq-stage'
    wflow.rules[0].apply(wflow)
    wflow.rules[1].apply(wflow)

    assert len(wflow.dag.nodes()) == 7

    assert wflow.rules[2].applicable(wflow) == True
    wflow.rules[2].apply(wflow)

    assert len(wflow.dag.nodes()) == 8

    pars =  wflow.dag.getNodeByName('reduce_complex_0').task.parameters

    assert list(pars.keys()) == ['grouped_inputs']
    assert set(pars['grouped_inputs'].keys()) == {'one','two'}

    assert pars['grouped_inputs']['one'] == [wflow.dag.getNodeByName('map_one_{}'.format(i)).result['outputfile'] for i in range(3)]
    assert pars['grouped_inputs']['two'] == [wflow.dag.getNodeByName('map_two_{}'.format(i)).result['outputfile'] for i in range(3)]



def test_multistepstage_zip_schedule_steps(batched_zip_mapreduce):
    wflow = batched_zip_mapreduce
    
    inputdata = [1,2,3,4,5]
    wflow.view().init({'input':inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stagespec['scheduler_type'] == 'multistep-stage'
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 + 3 # init + 3 batches (2,2,1)

def test_multistepstage_zip_schedule_steps(simple_mapreduce):
    wflow = simple_mapreduce
    
    inputdata = [1,2,3]
    wflow.view().init({'input':inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stagespec['scheduler_type'] == 'multistep-stage'
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
    assert wflow.rules[0].rule.stagespec['scheduler_type'] == 'multistep-stage'
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 + len(factor_one)*len(factor_two)  


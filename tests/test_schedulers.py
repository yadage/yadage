import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
from yadage.controllers import frommodel_controller


def test_singlestepstage_schedule_steps(local_helloworld_wflow):
    wflow = local_helloworld_wflow
    assert wflow.rules[0].rule.stagespec["scheduler_type"] == "singlestep-stage"
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().init({"par": "value"})

    # apply init
    assert wflow.rules[-1].applicable(wflow)
    wflow.rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1

    # get init result
    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    # recheck
    assert wflow.rules[0].applicable(wflow) == True
    wflow.rules[0].apply(wflow)


def test_nested_wflow(nested_wflow):
    wflow = nested_wflow

    inputdata = "world"
    wflow.view().init({"input": inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1

    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stagespec["scheduler_type"] == "singlestep-stage"
    assert len(wflow.view("").rules) == 3  # init nested higherscope
    wflow.rules[0].apply(wflow)


def test_singlestep_cases_first(singlestage_cases, foregroundasync_backend):
    wflow = singlestage_cases

    inputdata = {"par": 0}
    wflow.view().init(inputdata)

    right_taskspec = wflow.rules[0].rule.stagespec["cases"][0]["step"]
    assert wflow.rules[0].applicable(wflow) == False
    wflow.view().rules[-1].apply(wflow)

    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    assert len(wflow.dag.nodes()) == 1
    assert wflow.rules[0].applicable(wflow) == True
    wflow.rules[0].apply(wflow)

    assert len(wflow.dag.nodes()) == 2  # we scheduled is b/c it hits a good case

    assert wflow.dag.getNodeByName("hello_world").task.spec == right_taskspec


def test_value_registration(value_registering_workflow, foregroundasync_backend):
    wflow = value_registering_workflow

    inputdata = {"msg": "Hello World"}
    wflow.view().init(inputdata)

    assert wflow.rules[0].applicable(wflow) == False
    wflow.view().rules[-1].apply(wflow)  # apply init
    assert len(wflow.dag.nodes()) == 1  # init applied

    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    assert wflow.rules[0].applicable(wflow) == True  # first stage is good
    wflow.rules[0].apply(wflow)  # apply first stage

    value = wflow.view().getValue("ascopedfile")
    assert value
    assert type(value) == dict
    assert value["expression_type"] == "stage-output-selector"
    assert value["stages"] == "hello"
    assert value["output"] == "output"
    assert value["unwrap"] == True


def test_singlestep_cases_second(singlestage_cases):
    wflow = singlestage_cases
    inputdata = {"par": 1}
    wflow.view().init(inputdata)

    right_taskspec = wflow.rules[0].rule.stagespec["cases"][1]["step"]
    assert wflow.rules[0].applicable(wflow) == False
    wflow.view().rules[-1].apply(wflow)

    assert len(wflow.dag.nodes()) == 1

    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    assert wflow.rules[0].applicable(wflow) == True
    wflow.rules[0].apply(wflow)

    assert len(wflow.dag.nodes()) == 2  # we scheduled is b/c it hits a good case

    assert wflow.dag.getNodeByName("hello_world").task.spec == right_taskspec


def test_singlestep_cases_nochoice(singlestage_cases):
    wflow = singlestage_cases
    inputdata = {"par": -1}
    wflow.view().init(inputdata)

    right_taskspec = wflow.rules[0].rule.stagespec["cases"][1]["step"]
    assert wflow.rules[0].applicable(wflow) == False
    wflow.view().rules[-1].apply(wflow)

    assert len(wflow.dag.nodes()) == 1

    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    assert wflow.rules[0].applicable(wflow) == True
    wflow.rules[0].apply(wflow)

    assert len(wflow.dag.nodes()) == 1  # we scheduled is b/c it hits a good case
    assert wflow.dag.getNodeByName("hello_world") == None


def test_multistepstage_schedule_wflows(nested_mapreduce_wflow):
    wflow = nested_mapreduce_wflow

    inputdata = [1, 2, 3]
    wflow.view().init({"input": inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1

    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stagespec["scheduler_type"] == "multistep-stage"
    assert len(wflow.view("").rules) == 3  # init map reduce
    wflow.rules[0].apply(wflow)
    assert len(wflow.view("/map/0").rules) == 3 + 2 * len(
        inputdata
    )  # 3x init and stage rules


def test_jq_stage(jqworkflow):
    wflow = jqworkflow

    inputdata = [1, 2, 3]
    wflow.view().init({"parone": inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1

    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stagespec["scheduler_type"] == "jq-stage"
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 + len(inputdata)


def test_jqnodestruct_stage(jqnodestruct):
    wflow = jqnodestruct

    inputdata = {"scatterone": ["a", "b", "c"], "scatertwo": [1, 2, 3]}

    wflow.view().init(inputdata)
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1

    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[1].applicable(wflow) == True
    # assert wflow.rules[0].rule.stagespec['scheduler_type'] == 'jq-stage'
    wflow.rules[0].apply(wflow)
    wflow.rules[1].apply(wflow)

    assert len(wflow.dag.nodes()) == 7

    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    assert wflow.rules[2].applicable(wflow) == True
    wflow.rules[2].apply(wflow)

    assert len(wflow.dag.nodes()) == 8

    pars = wflow.dag.getNodeByName("reduce_complex_0").task.parameters

    assert list(pars.json().keys()) == ["grouped_inputs"]
    assert set(pars["grouped_inputs"].keys()) == {"one", "two"}

    assert pars["grouped_inputs"]["one"] == [
        wflow.dag.getNodeByName("map_one_{}".format(i)).result["outputfile"]
        for i in range(3)
    ]
    assert pars["grouped_inputs"]["two"] == [
        wflow.dag.getNodeByName("map_two_{}".format(i)).result["outputfile"]
        for i in range(3)
    ]


def test_multistepstage_zip_schedule_steps(batched_zip_mapreduce):
    wflow = batched_zip_mapreduce

    inputdata = [1, 2, 3, 4, 5]
    wflow.view().init({"input": inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1
    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stagespec["scheduler_type"] == "multistep-stage"
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 + 3  # init + 3 batches (2,2,1)


def test_multistepstage_zip_schedule_steps(simple_mapreduce):
    wflow = simple_mapreduce

    inputdata = [1, 2, 3]
    wflow.view().init({"input": inputdata})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1

    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stagespec["scheduler_type"] == "multistep-stage"
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 + len(inputdata)


def test_multistepstage_cartesian_schedule_steps(cartesian_mapreduce):
    wflow = cartesian_mapreduce

    factor_one = [1, 2, 3]
    factor_two = [4, 5]
    wflow.view().init({"factor_one": factor_one, "factor_two": factor_two})
    assert wflow.rules[0].applicable(wflow) == False

    wflow.view().rules[-1].apply(wflow)
    assert len(wflow.dag.nodes()) == 1

    c = frommodel_controller("", {}, wflow)
    c.sync_backend()

    assert wflow.rules[0].applicable(wflow) == True
    assert wflow.rules[0].rule.stagespec["scheduler_type"] == "multistep-stage"
    wflow.rules[0].apply(wflow)
    assert len(wflow.dag.nodes()) == 1 + len(factor_one) * len(factor_two)

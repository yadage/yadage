import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
from packtivity.statecontexts.posixfs_context import LocalFSState
from yadage.state_providers.localposix import LocalFSProvider
from yadage.controllers import frommodel_controller


def test_multistepstage_schedule_wflows(localfs_state_provider):
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/nestedmapreduce"
    )
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)

    inputdata = [1, 2, 3]
    wflow.view().init({"input": inputdata})
    assert wflow.view().getRule(name="map").applicable(wflow) == False

    wflow.view().getRule(name="init").apply(wflow)
    assert len(wflow.dag.nodes()) == 1

    frommodel_controller("", {}, wflow).sync_backend()
    assert wflow.view().getRule(name="map").applicable(wflow) == True

    wflow.view().getRule(name="map").apply(wflow)
    assert wflow.view().getRule(name="reduce").applicable(wflow) == False

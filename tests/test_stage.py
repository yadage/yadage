import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
from yadage.stages import JsonStage
from packtivity.statecontexts.posixfs_context import LocalFSState
from yadage.state_providers.localposix import LocalFSProvider
from yadage.controllers import frommodel_controller


def test_applicable():
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/local-helloworld"
    )
    wflow = YadageWorkflow.createFromJSON(
        data, LocalFSProvider(LocalFSState(["/workdir"]), ensure=False)
    )
    wflow.view().init({"par": "value"})
    assert wflow.rules[-1].applicable(wflow) == True


def test_apply():
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/local-helloworld"
    )
    wflow = YadageWorkflow.createFromJSON(
        data, LocalFSProvider(LocalFSState(["/workdir"]), ensure=False)
    )
    wflow.view().init({"par": "value"})
    assert wflow.rules[-1].applicable(wflow) == True
    wflow.rules[-1].apply(wflow)
    frommodel_controller("", {}, wflow).sync_backend()
    assert wflow.rules[0].applicable(wflow) == True
    wflow.rules[0].apply(wflow)


def test_serialize_deserialize():
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/local-helloworld"
    )
    wflow = YadageWorkflow.createFromJSON(
        data, LocalFSProvider(LocalFSState(["/workdir"]), ensure=False)
    )
    wflow.view().init({"hello": "world"})
    assert wflow.rules[0].rule.json()
    assert wflow.rules[1].rule.json()

    assert (
        JsonStage.fromJSON(wflow.rules[0].rule.json()).json()
        == wflow.rules[0].rule.json()
    )

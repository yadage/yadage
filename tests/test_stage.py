import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
from yadage.stages import jsonStage,initStage
from packtivity.statecontexts.posixfs_context import LocalFSProvider,LocalFSState

def test_applicable():
    data  = yadage.workflow_loader.workflow('workflow.yml','tests/testspecs/local-helloworld')
    wflow = YadageWorkflow.createFromJSON(data,LocalFSProvider(LocalFSState(['/workdir']), ensure = False))
    assert wflow.rules[0].applicable(wflow) == True

def test_apply():
    data  = yadage.workflow_loader.workflow('workflow.yml','tests/testspecs/local-helloworld')
    wflow = YadageWorkflow.createFromJSON(data,LocalFSProvider(LocalFSState(['/workdir']), ensure = False))
    assert wflow.rules[0].applicable(wflow) == True
    wflow.rules[0].apply(wflow)

def test_serialize_deserialize():
    data  = yadage.workflow_loader.workflow('workflow.yml','tests/testspecs/local-helloworld')
    wflow = YadageWorkflow.createFromJSON(data,LocalFSProvider(LocalFSState(['/workdir']), ensure = False))
    wflow.view().init({'hello':'world'})
    assert wflow.rules[0].rule.json()
    assert wflow.rules[1].rule.json()

    assert jsonStage.fromJSON(wflow.rules[0].rule.json())
    assert jsonStage.fromJSON(wflow.rules[1].rule.json())

    assert jsonStage.fromJSON(wflow.rules[0].rule.json()).json()
    assert jsonStage.fromJSON(wflow.rules[1].rule.json()).json()

    # assert jsonStage.fromJSON(wflow.rules[0].rule.json()).json() == wflow.rules[0].rule.json()
    # assert initStage.fromJSON(wflow.rules[1].rule.json()).json() == wflow.rules[1].rule.json()

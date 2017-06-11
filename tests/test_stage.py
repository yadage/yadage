import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
from yadage.stages import jsonStage,initStage
import packtivity.statecontexts.posixfs_context as statecontext

def test_applicable():
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/local-helloworld')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context('/workdir'))
    assert wflow.rules[0].applicable(wflow) == True

def test_apply():
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/local-helloworld')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context('/workdir'))
    assert wflow.rules[0].applicable(wflow) == True
    wflow.rules[0].apply(wflow)

def test_serialize_deserialize():
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/local-helloworld')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context('/workdir'))
    wflow.view().init({'hello':'world'})
    assert jsonStage.fromJSON(wflow.rules[0].rule.json()).json() == wflow.rules[0].rule.json()
    assert initStage.fromJSON(wflow.rules[1].rule.json()).json() == wflow.rules[1].rule.json()

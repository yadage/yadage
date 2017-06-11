from yadage.wflow import YadageWorkflow
import packtivity.statecontexts.posixfs_context as statecontext
import json
import yadage.workflow_loader


def test_create():
    wflow = YadageWorkflow()


def test_view():
    wflow = YadageWorkflow()
    wflow.view()

def test_serialize():
    wflow = YadageWorkflow()
    data = wflow.json()
    json.dumps(data)

def test_deserialize():
    data = {'rules': [], 'applied': [], 'dag': {'nodes': [], 'edges': []}, 'stepsbystage': {}, 'bookkeeping': {}}
    wflow = YadageWorkflow.fromJSON(data)
    assert data == wflow.json()

def test_create_from_data():
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/local-helloworld')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context('/workdir'))

def test_serialize_deserialize():
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/local-helloworld')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context('/workdir'))
    assert YadageWorkflow.fromJSON(wflow.json()).json() == wflow.json()
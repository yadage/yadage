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

def test_create_from_data(local_helloworld_wflow):
    pass

def test_serialize_deserialize(local_helloworld_wflow):
    wflow = local_helloworld_wflow
    assert YadageWorkflow.fromJSON(wflow.json()).json() == wflow.json()
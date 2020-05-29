import pytest
from yadage.wflownode import YadageNode
from yadage.tasks import packtivity_task
from yadage.controllers import YadageController


def test_create(basic_packtivity_spec, localfs_state):
    step = packtivity_task("myname", basic_packtivity_spec, localfs_state)
    node = YadageNode("myname", step, "identiifer")


def test_result_prepub(basic_packtivity_spec, localfs_state):
    step = packtivity_task(
        "myname",
        basic_packtivity_spec,
        localfs_state,
        {"outputfile": "world", "par": "value"},
    )
    node = YadageNode("myname", step, "identiifer")

    c = YadageController(None)
    node.expected_result = c.prepublishing_backend.prepublish(
        node.task.spec, node.task.parameters.json(), node.task.state
    )
    assert node.has_result() == True
    assert node.result == node.expected_result

    node.readfromresult("") == node.result
    node.readfromresult("/outputfile") == node.result["outputfile"]

    another_step = packtivity_task("another", basic_packtivity_spec, localfs_state)
    node.readfromresult("/outputfile", another_step.inputs)
    assert another_step.inputs[-1].stepid == node.identifier
    assert another_step.inputs[-1].pointer.path == "/outputfile"


def test_serialize_deserialize(basic_packtivity_spec, localfs_state):
    step = packtivity_task("myname", basic_packtivity_spec, localfs_state)
    packtivity_task.fromJSON(step.json()).json() == step.json()


def test_noresult(dynamic_packtivity_spec, localfs_state):
    step = packtivity_task(
        "myname",
        dynamic_packtivity_spec,
        localfs_state,
        {"localname": "hello", "source": "world"},
    )
    node = YadageNode("myname", step, "identiifer")
    assert node.has_result() == False
    node.readfromresult("", failsilently=True) == None
    with pytest.raises(RuntimeError):
        node.readfromresult("") == None


def test_repr(basic_packtivity_spec, localfs_state):
    step = packtivity_task(
        "myname",
        basic_packtivity_spec,
        localfs_state,
        {"outputfile": "world", "par": "value"},
    )
    node = YadageNode("myname", step, "identiifer")
    assert repr(node)

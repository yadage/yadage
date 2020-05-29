import json
from click.testing import CliRunner
from yadage.controllers import setup_controller, PersistentController
from yadage.wflowstate import load_model_fromstring, FileBackedModel
from yadage.wflow import YadageWorkflow
from yadage.controllers import YadageController


def test_setup_filebacked(tmpdir, local_helloworld_wflow):
    thefile = tmpdir.join("state.json")
    thefile.write(json.dumps(local_helloworld_wflow.json()))
    model = load_model_fromstring("filebacked:" + str(thefile))
    assert type(model) == FileBackedModel
    ctrl = setup_controller(model)
    assert type(ctrl) == PersistentController


def test_setup_inmem(tmpdir, local_helloworld_wflow):
    model = load_model_fromstring("inmem", initmodel=local_helloworld_wflow)
    assert type(model) == YadageWorkflow
    ctrl = setup_controller(model)
    assert type(ctrl) == YadageController


def test_from_pythonctrl(tmpdir, local_helloworld_wflow):
    thefile = tmpdir.join("state.json")
    thefile.write(json.dumps(local_helloworld_wflow.json()))
    model = load_model_fromstring("filebacked:" + str(thefile))
    assert type(model) == FileBackedModel
    ctrl = setup_controller(
        model,
        controller="py:yadage.controllers:PersistentController",
        ctrlopts={"pass_model": True},
    )
    assert type(ctrl) == PersistentController
    assert ctrl.model is not None

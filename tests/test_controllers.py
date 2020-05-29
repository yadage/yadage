from yadage.controllers import PersistentController
from yadage.wflowstate import FileBackedModel
from yadage.state_providers import load_provider


def test_persistent_controller(
    tmpdir, local_helloworld_wflow_w_init, foregroundasync_backend
):

    model = FileBackedModel(
        filename=str(tmpdir.join("wflowstate")), initmodel=local_helloworld_wflow_w_init
    )

    pers_ctrl = PersistentController(model, foregroundasync_backend)

    assert pers_ctrl.submittable_nodes() == []
    assert set(pers_ctrl.applicable_rules()) == set(
        [local_helloworld_wflow_w_init.view().getRule("init").identifier]
    )

    pers_ctrl.apply_rules(pers_ctrl.applicable_rules())

    assert len(pers_ctrl.submittable_nodes()) == 1
    pers_ctrl.submit_nodes(pers_ctrl.submittable_nodes())

    assert set(pers_ctrl.applicable_rules()) == set(
        [local_helloworld_wflow_w_init.view().getRule("hello_world").identifier]
    )

    pers_ctrl.apply_rules(pers_ctrl.applicable_rules())
    assert len(pers_ctrl.submittable_nodes()) == 1
    pers_ctrl.submit_nodes(pers_ctrl.submittable_nodes())

    model = FileBackedModel(filename=str(tmpdir.join("wflowstate")),)
    pers_ctrl = PersistentController(model, foregroundasync_backend)

    # assert pers_ctrl.adageobj.dag.getNode(pers_ctrl.submittable_nodes()[0]).task.state.json() == 11
    # pers_ctrl.submit_nodes(pers_ctrl.submittable_nodes())

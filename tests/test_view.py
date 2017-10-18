import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
from yadage.stages import OffsetStage
from packtivity.statecontexts import load_provider
from packtivity.statecontexts.posixfs_context import LocalFSProvider,LocalFSState

def test_init():
    data  = yadage.workflow_loader.workflow('workflow.yml','tests/testspecs/nestedmapreduce')
    wflow = YadageWorkflow.createFromJSON(data,LocalFSProvider(LocalFSState(['/workdir']), ensure = False))
    view  = wflow.view()

    nrules_before = len(wflow.rules)
    view.init({'hello':'world'})
    nrules_after = len(wflow.rules)
    assert nrules_before + 1 == nrules_after
    view.rules[-1].apply(wflow)

    matches = view.query('init',view.steps)
    assert len(matches) == 1

def test_serialize_offsetstage():
    data  = yadage.workflow_loader.workflow('workflow.yml','tests/testspecs/nestedmapreduce')
    wflow = YadageWorkflow.createFromJSON(data,LocalFSProvider(LocalFSState(['/workdir']), ensure = False))
    wflow.view().init({'input':[1,2,3]})
    for x in wflow.rules:
        assert OffsetStage.fromJSON(x.json(), load_provider).json() == x.json()

def test_getRule():
    data  = yadage.workflow_loader.workflow('workflow.yml','tests/testspecs/nestedmapreduce')
    wflow = YadageWorkflow.createFromJSON(data,LocalFSProvider(LocalFSState(['/workdir']), ensure = False))
    wflow.view().init({'input':[1,2,3]})

    assert wflow.view().getRule(identifier = wflow.rules[0].identifier) == wflow.rules[0]
    assert wflow.view().getRule(name = wflow.rules[0].rule.name, offset = '') == wflow.rules[0]
    assert wflow.view().getRule(name = 'nonexistent') == None

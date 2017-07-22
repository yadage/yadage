import os
from yadage.steering_object import YadageSteering
from yadage.steering_api import steering_ctx
from yadage.utils import prepare_workdir_from_archive
from yadage.reset import reset_steps, collective_downstream

def test_steer(tmpdir,multiproc_backend):
    ys = YadageSteering()
    ys.prepare_workdir(os.path.join(str(tmpdir),'workdir'))
    ys.init_workflow('workflow.yml', 'tests/testspecs/nestedmapreduce',{'input': [1,2,3]})

    ys.adage_argument(default_trackers = False)
    ys.run_adage(multiproc_backend)

def test_context(tmpdir,multiproc_backend):
    workdir = os.path.join(str(tmpdir),'workdir')
    with steering_ctx(workdir, 'workflow.yml', {'input':[1,2,3]}, 'tests/testspecs/nestedmapreduce', multiproc_backend) as ys:
        ys.adage_argument(default_trackers = False)

def test_inparchive(tmpdir,multiproc_backend):
    workdir = os.path.join(str(tmpdir),'workdir')
    initdir = prepare_workdir_from_archive(workdir, 'file://{}/tests/testspecs/dynamic_glob/inputs/three_files.zip'.format(os.path.abspath(os.curdir)))
    with steering_ctx(workdir, 'workflow_frominit.yml', {'inputfiles':'*.txt'}, 'tests/testspecs/dynamic_glob', multiproc_backend, initdir = initdir) as ys:
        ys.adage_argument(default_trackers = False)


def test_reset(tmpdir,multiproc_backend):
    ys = YadageSteering()
    ys.prepare_workdir(os.path.join(str(tmpdir),'workdir'))
    ys.init_workflow('workflow.yml', 'tests/testspecs/nestedmapreduce',{'input': [1,2,3]})

    ys.adage_argument(default_trackers = False)
    ys.run_adage(multiproc_backend)

    assert tmpdir.join('workdir/reduce/output').check() == True

    to_remove = [n.identifier for n in ys.controller.adageobj.view().getSteps('map.[*].stage1')]
    downstream = collective_downstream(ys.controller.adageobj,to_remove)
    reset_steps(ys.controller.adageobj,downstream + to_remove)

    assert tmpdir.join('workdir/reduce/output').check() == False
    ys.run_adage(multiproc_backend)
    assert tmpdir.join('workdir/reduce/output').check() == True



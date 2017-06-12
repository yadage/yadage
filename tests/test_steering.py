import os
from yadage.steering_object import YadageSteering
from yadage.steering_api import steering_ctx
from yadage.clihelpers import prepare_workdir_from_archive
from yadage.reset import reset_steps, collective_downstream
def test_steer(tmpdir,multiproc_backend):
    ys = YadageSteering()
    ys.prepare_workdir(os.path.join(str(tmpdir),'workdir'))
    ys.init_workflow('workflow.yml', 'testspecs/nestedmapreduce',{'input': [1,2,3]})

    ys.adage_argument(default_trackers = False)
    ys.run_adage(multiproc_backend)

def test_context(tmpdir,multiproc_backend):
    workdir = os.path.join(str(tmpdir),'workdir')
    with steering_ctx(workdir, 'workflow.yml', {'input':[1,2,3]}, 'testspecs/nestedmapreduce', multiproc_backend) as ys:
        ys.adage_argument(default_trackers = False)


import logging
logging.basicConfig(level = logging.INFO)
def test_cached(tmpdir,checksum_cached_multiproc):
    workdir = os.path.join(str(tmpdir),'workdir')
    with steering_ctx(workdir, 'workflow.yml', {}, 'testspecs/local-helloworld', checksum_cached_multiproc) as ys:
        ys.adage_argument(default_trackers = False)

    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()

    checksum_cached_multiproc.backends['packtivity'].cache.todisk()


    with steering_ctx(workdir, 'workflow.yml', {}, 'testspecs/local-helloworld', checksum_cached_multiproc, accept_existing_workdir = True) as ys:
        ys.adage_argument(default_trackers = False)

    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()
    tmpdir.join('workdir/hello_world/hello_world.txt').remove()

    with steering_ctx(workdir, 'workflow.yml', {}, 'testspecs/local-helloworld', checksum_cached_multiproc, accept_existing_workdir = True) as ys:
        ys.adage_argument(default_trackers = False)

def test_inparchive(tmpdir,multiproc_backend):
    workdir = os.path.join(str(tmpdir),'workdir')
    initdir = prepare_workdir_from_archive(workdir, 'file://{}/testspecs/dynamic_glob/inputs/three_files.zip'.format(os.path.abspath(os.curdir)))
    with steering_ctx(workdir, 'workflow_frominit.yml', {'inputfiles':'*.txt'}, 'testspecs/dynamic_glob', multiproc_backend, initdir = initdir) as ys:
        ys.adage_argument(default_trackers = False)


def test_reset(tmpdir,multiproc_backend):
    ys = YadageSteering()
    ys.prepare_workdir(os.path.join(str(tmpdir),'workdir'))
    ys.init_workflow('workflow.yml', 'testspecs/nestedmapreduce',{'input': [1,2,3]})

    ys.adage_argument(default_trackers = False)
    ys.run_adage(multiproc_backend)

    assert tmpdir.join('workdir/reduce/output').check() == True

    to_remove = [n.identifier for n in ys.controller.adageobj.view().getSteps('map.[*].stage1')]
    downstream = collective_downstream(ys.controller.adageobj,to_remove)
    reset_steps(ys.controller.adageobj,downstream + to_remove)

    print downstream + to_remove

    assert tmpdir.join('workdir/reduce/output').check() == False
    ys.run_adage(multiproc_backend)
    assert tmpdir.join('workdir/reduce/output').check() == True


#    yadage-run work3 workflow_frominit.yml frominit_init.yml -a file:///Users/lukas/Code/yadagedev/yadage/tests/testspecs/dynamic_glob/inputs/three_files.zip
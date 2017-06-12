import os
from yadage.steering_object import YadageSteering
from yadage.steering_api import steering_ctx

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

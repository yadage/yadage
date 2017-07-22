import os
import pytest
from yadage.utils import setupbackend_fromstring
from yadage.steering_api import steering_ctx

def test_cached(tmpdir,checksum_cached_multiproc):
    workdir = os.path.join(str(tmpdir),'workdir')
    with steering_ctx(workdir, 'workflow.yml', {}, 'tests/testspecs/local-helloworld', checksum_cached_multiproc) as ys:
        ys.adage_argument(default_trackers = False)

    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()

    checksum_cached_multiproc.backends['packtivity'].cache.todisk()


    with steering_ctx(workdir, 'workflow.yml', {}, 'tests/testspecs/local-helloworld', checksum_cached_multiproc, accept_existing_workdir = True) as ys:
        ys.adage_argument(default_trackers = False)

    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()
    tmpdir.join('workdir/hello_world/hello_world.txt').remove()

    with steering_ctx(workdir, 'workflow.yml', {}, 'tests/testspecs/local-helloworld', checksum_cached_multiproc, accept_existing_workdir = True) as ys:
        ys.adage_argument(default_trackers = False)

def test_nonexisting_cache():
    with pytest.raises(RuntimeError):
        setupbackend_fromstring('multiproc:4', cacheconfig = 'nonexistent:config')

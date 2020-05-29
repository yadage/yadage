import os
import pytest
from yadage.utils import setupbackend_fromstring
from yadage.steering_api import steering_ctx


def test_cached(tmpdir, checksum_cached_multiproc):
    workdir = os.path.join(str(tmpdir), "workdir")

    def run_workflow():
        with steering_ctx(
            "local:" + workdir,
            "workflow.yml",
            {"par": "value"},
            "tests/testspecs/local-helloworld",
            checksum_cached_multiproc,
            accept_metadir=True,
        ) as ys:
            ys.adage_argument(default_trackers=False)

    # run initial workflow and make sure resutl file exists
    run_workflow()
    assert tmpdir.join("workdir/hello_world/hello_world.txt").check()

    checksum_cached_multiproc.backends["packtivity"].cache.todisk()

    # disable primary backend, since wflow is cached, should not have to call
    checksum_cached_multiproc.backends["packtivity"].primary_enabled = False
    run_workflow()
    assert tmpdir.join("workdir/hello_world/hello_world.txt").check()

    # if we remove a fail, cache should be invalidated and with disabled primary
    # backend we should hit an exception
    tmpdir.join("workdir/hello_world/hello_world.txt").remove()
    with pytest.raises(RuntimeError):
        run_workflow()

    # when we enable it again, all should be fine
    checksum_cached_multiproc.backends["packtivity"].primary_enabled = True
    run_workflow()


def test_cached_fromstring(tmpdir):
    workdir = os.path.join(str(tmpdir), "workdir")

    def run_workflow():
        backend = setupbackend_fromstring("multiproc:auto")
        with steering_ctx(
            "local:" + workdir,
            "workflow.yml",
            {"par": "value"},
            "tests/testspecs/local-helloworld",
            backend,
            cache="checksums",
        ) as ys:
            ys.adage_argument(default_trackers=False)

        backend.backends["packtivity"].cache.todisk()

    # run initial workflow and make sure resutl file exists
    run_workflow()
    assert tmpdir.join("workdir/hello_world/hello_world.txt").check()

    # run initial workflow and make sure result file exists
    run_workflow()
    assert tmpdir.join("workdir/hello_world/hello_world.txt").check()


def test_nonexisting_cache():
    with pytest.raises(RuntimeError):
        backend = setupbackend_fromstring("multiproc:4")
        backend.enable_cache("nonexistent:config")

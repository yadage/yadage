import pytest

import os
import yadage.workflow_loader
from packtivity.statecontexts.posixfs_context import LocalFSState
import packtivity.utils
from yadage.state_providers.localposix import LocalFSProvider
from yadage.utils import setupbackend_fromstring
from yadage.wflow import YadageWorkflow


@pytest.fixture()
def localfs_state(tmpdir):
    return LocalFSState([tmpdir.dirname])


@pytest.fixture()
def localfs_state_provider(tmpdir):
    return LocalFSProvider(LocalFSState([tmpdir.dirname]), ensure=True)


@pytest.fixture()
def basic_packtivity_spec(tmpdir):
    return packtivity.utils.load_packtivity(
        "tests/testspecs/local-helloworld/workflow.yml#/stages/0/scheduler/step"
    )


@pytest.fixture()
def dynamic_packtivity_spec(tmpdir):
    return packtivity.utils.load_packtivity(
        "tests/testspecs/dynamic_glob/steps.yml#/acquire"
    )


@pytest.fixture()
def nested_mapreduce_wflow(tmpdir, localfs_state_provider):
    """a workflow object with horizontally scalable map stage scheduling sub-workflows"""
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/nestedmapreduce"
    )
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)
    return wflow


@pytest.fixture()
def nested_wflow(tmpdir, localfs_state_provider):
    """a workflow object with horizontally scalable map stage scheduling sub-workflows"""
    data = yadage.workflow_loader.workflow("workflow.yml", "tests/testspecs/nested")
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)
    return wflow


@pytest.fixture()
def local_helloworld_wflow(tmpdir, localfs_state_provider):
    """a workflow object with horizontally scalable map stage scheduling sub-workflows"""
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/local-helloworld"
    )
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)
    return wflow


@pytest.fixture()
def local_helloworld_wflow_w_init(tmpdir, localfs_state_provider):
    """a workflow object with horizontally scalable map stage scheduling sub-workflows"""
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/local-helloworld"
    )
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)
    wflow.view().init({"par": "hello"})
    return wflow


@pytest.fixture()
def cartesian_mapreduce(tmpdir, localfs_state_provider):
    """a workflow object with horizontally scalable map stage scheduling sub-workflows"""
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/cartesian_mapreduce"
    )
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)
    return wflow


@pytest.fixture()
def batched_zip_mapreduce(tmpdir, localfs_state_provider):
    """a workflow object with horizontally scalable map stage scheduling sub-workflows"""
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/mapreduce_batched"
    )
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)
    return wflow


@pytest.fixture()
def jqworkflow(tmpdir, localfs_state_provider):
    """a workflow object with horizontally scalable map stage scheduling sub-workflows"""
    data = yadage.workflow_loader.workflow("workflow.yml", "tests/testspecs/jqwflow")
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)
    return wflow


@pytest.fixture()
def jqnodestruct(tmpdir, localfs_state_provider):
    """a workflow object with horizontally scalable map stage scheduling sub-workflows"""
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/jqnodestruct"
    )
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)
    return wflow


@pytest.fixture()
def simple_mapreduce(tmpdir, localfs_state_provider):
    """a workflow object with horizontally scalable map stage scheduling sub-workflows"""
    data = yadage.workflow_loader.workflow("workflow.yml", "tests/testspecs/mapreduce")
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)
    return wflow


@pytest.fixture()
def singlestage_cases(tmpdir, localfs_state_provider):
    """a workflow object with horizontally scalable map stage scheduling sub-workflows"""
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/casesflow/single"
    )
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)
    return wflow


@pytest.fixture()
def value_registering_workflow(tmpdir, localfs_state_provider):
    """a workflow object with horizontally scalable map stage scheduling sub-workflows"""
    data = yadage.workflow_loader.workflow(
        "workflow.yml", "tests/testspecs/registering_values"
    )
    wflow = YadageWorkflow.createFromJSON(data, localfs_state_provider)
    return wflow


@pytest.fixture()
def multiproc_backend():
    backend = setupbackend_fromstring("multiproc:4")
    return backend


@pytest.fixture()
def foregroundasync_backend():
    backend = setupbackend_fromstring("foregroundasync")
    return backend


@pytest.fixture()
def checksum_cached_multiproc(tmpdir):
    cache = str(tmpdir.join("cache.json"))
    backend = setupbackend_fromstring("multiproc:4")
    backend.enable_cache("checksums:" + cache)
    return backend

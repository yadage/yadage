import pytest

import os
import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
from packtivity.statecontexts.posixfs_context import LocalFSProvider,LocalFSState
import packtivity.utils
from yadage.utils import setupbackend_fromstring

@pytest.fixture()
def localfs_state(tmpdir):
    return LocalFSState(tmpdir.dirname)

@pytest.fixture()
def localfs_state_provider(tmpdir):
    return LocalFSProvider(LocalFSState(tmpdir.dirname))

@pytest.fixture()
def basic_packtivity_spec(tmpdir):
    return packtivity.utils.load_packtivity('tests/testspecs/local-helloworld/workflow.yml#/stages/0/scheduler/step')

@pytest.fixture()
def nested_mapreduce_wflow(tmpdir,localfs_state_provider):
    '''a workflow object with horizontally scalable map stage scheduling sub-workflows'''
    data  = yadage.workflow_loader.workflow('workflow.yml','tests/testspecs/nestedmapreduce')
    wflow = YadageWorkflow.createFromJSON(data,localfs_state_provider)
    return wflow

@pytest.fixture()
def local_helloworld_wflow(tmpdir,localfs_state_provider):
    '''a workflow object with horizontally scalable map stage scheduling sub-workflows'''
    data  = yadage.workflow_loader.workflow('workflow.yml','tests/testspecs/local-helloworld')
    wflow = YadageWorkflow.createFromJSON(data,localfs_state_provider)
    return wflow

@pytest.fixture()
def cartesian_mapreduce(tmpdir,localfs_state_provider):
    '''a workflow object with horizontally scalable map stage scheduling sub-workflows'''
    data  = yadage.workflow_loader.workflow('workflow.yml','tests/testspecs/cartesian_mapreduce')
    wflow = YadageWorkflow.createFromJSON(data,localfs_state_provider)
    return wflow

@pytest.fixture()
def simple_mapreduce(tmpdir,localfs_state_provider):
    '''a workflow object with horizontally scalable map stage scheduling sub-workflows'''
    data  = yadage.workflow_loader.workflow('workflow.yml','tests/testspecs/mapreduce')
    wflow = YadageWorkflow.createFromJSON(data,localfs_state_provider)
    return wflow

@pytest.fixture()
def multiproc_backend():
    backend = setupbackend_fromstring('multiproc:4')
    return backend

@pytest.fixture()
def checksum_cached_multiproc(tmpdir):
    cache   = str(tmpdir.join('cache.json'))
    backend = setupbackend_fromstring('multiproc:4', cacheconfig = 'checksums:'+cache)
    return backend
import pytest

import os
import yadage.workflow_loader
from yadage.wflow import YadageWorkflow
import packtivity.statecontexts.posixfs_context as statecontext

from yadage.clihelpers import setupbackend_fromstring

@pytest.fixture()
def nested_mapreduce_wflow(tmpdir):
    '''a workflow object with horizontally scalable map stage scheduling sub-workflows'''
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/nestedmapreduce')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context(tmpdir.dirname))
    return wflow

@pytest.fixture()
def local_helloworld_wflow(tmpdir):
    '''a workflow object with horizontally scalable map stage scheduling sub-workflows'''
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/local-helloworld')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context(tmpdir.dirname))
    return wflow

@pytest.fixture()
def cartesian_mapreduce(tmpdir):
    '''a workflow object with horizontally scalable map stage scheduling sub-workflows'''
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/cartesian_mapreduce')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context(tmpdir.dirname))
    return wflow

@pytest.fixture()
def simple_mapreduce(tmpdir):
    '''a workflow object with horizontally scalable map stage scheduling sub-workflows'''
    data  = yadage.workflow_loader.workflow('workflow.yml','testspecs/mapreduce')
    wflow = YadageWorkflow.createFromJSON(data,statecontext.make_new_context(tmpdir.dirname))
    return wflow

@pytest.fixture()
def multiproc_backend():
    backend = setupbackend_fromstring('multiproc:4')
    return backend
import pytest
import yadage.workflow_loader

def test_load_hello_world():
	yadage.workflow_loader.workflow('workflow.yml','testspecs/local-helloworld')

def test_load_failt():
	with pytest.raises(RuntimeError):
		yadage.workflow_loader.workflow('non_existent.yml','testspecs/local-helloworld')
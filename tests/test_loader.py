import pytest
import yadage.workflow_loader
import jsonschema.exceptions


def test_load_hello_world():
    yadage.workflow_loader.workflow("workflow.yml", "tests/testspecs/local-helloworld")


def test_load_failt():
    with pytest.raises(RuntimeError):
        yadage.workflow_loader.workflow(
            "non_existent.yml", "tests/testspecs/local-helloworld"
        )


def test_validate_fail():
    with pytest.raises(jsonschema.exceptions.ValidationError):
        yadage.workflow_loader.validate({})

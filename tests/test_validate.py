import yadage.validator_workflow
from click.testing import CliRunner
import yadage.manualcli
import os
import pytest

def test_validator():
    runner = CliRunner()
    result = runner.invoke(yadage.validator_workflow.main,['workflow.yml','tests/testspecs/local-helloworld'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.validator_workflow.main,['workflow.yml','tests/testspecs/local-helloworld','-s'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.validator_workflow.main,['workflow.yml','tests/testspecs/nestedmapreduce','-s'])
    assert result.exit_code == 0

def test_validator_noteven():
    runner = CliRunner()
    result = runner.invoke(yadage.validator_workflow.main,['unknown','unknown'])
    assert result.exit_code == 1

def test_validator_invalid():
    runner = CliRunner()
    result = runner.invoke(yadage.validator_workflow.main,['invalid_spec.yml','testspecs'])
    assert result.exit_code == 1

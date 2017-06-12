import yadage.validator_workflow
from click.testing import CliRunner
import yadage.manualcli
import os
import pytest

def test_validator():
    runner = CliRunner()
    result = runner.invoke(yadage.validator_workflow.main,['workflow.yml','testspecs/local-helloworld'])
    assert result.exit_code == 0


    result = runner.invoke(yadage.validator_workflow.main,['workflow.yml','testspecs/local-helloworld','-s'])
    assert result.exit_code == 0

def test_validator_invalid():
    runner = CliRunner()
    result = runner.invoke(yadage.validator_workflow.main,['unknown','unknown'])
    assert result.exit_code == 1

from click.testing import CliRunner
import yadage.utilcli
import os

def test_testsel():
    runner = CliRunner()
    result = runner.invoke(yadage.utilcli.testsel)

def test_viz():
    runner = CliRunner()
    result = runner.invoke(yadage.utilcli.viz)

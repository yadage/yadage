from click.testing import CliRunner
import yadage.manualcli
import os

def test_init():
    runner = CliRunner()
    result = runner.invoke(yadage.manualcli.init)

def test_apply():
    runner = CliRunner()
    result = runner.invoke(yadage.manualcli.apply)

def test_submit():
    runner = CliRunner()
    result = runner.invoke(yadage.manualcli.submit)

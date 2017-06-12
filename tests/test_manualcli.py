from click.testing import CliRunner
import yadage.manualcli
import os

def test_init(tmpdir):
    workdir = os.path.join(str(tmpdir),'workdir')
    runner = CliRunner()
    result = runner.invoke(yadage.manualcli.init,[workdir,'workflow.yml','-t','testspecs/local-helloworld'])

def test_apply():
    runner = CliRunner()
    result = runner.invoke(yadage.manualcli.apply)

def test_submit():
    runner = CliRunner()
    result = runner.invoke(yadage.manualcli.submit)

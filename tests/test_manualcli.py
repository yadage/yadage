from click.testing import CliRunner
import yadage.manualcli
import os


def test_manual(tmpdir):
    runner = CliRunner()
    workdir   = os.path.join(str(tmpdir),'workdir')
    statefile = os.path.join(str(tmpdir),'state.json')
    result = runner.invoke(yadage.manualcli.init,[workdir,'workflow.yml','-t','testspecs/local-helloworld','-s','filebacked:'+statefile])
    result = runner.invoke(yadage.manualcli.apply,['-s','filebacked:'+statefile,'-n','/hello_world'])
    result = runner.invoke(yadage.manualcli.show,['-s','filebacked:'+statefile])
    assert result.exit_code == 0

def test_submit():
    runner = CliRunner()
    result = runner.invoke(yadage.manualcli.submit)

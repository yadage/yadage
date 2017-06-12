from click.testing import CliRunner
import yadage.manualcli
import os


def test_manual(tmpdir):
    runner = CliRunner()
    workdir   = os.path.join(str(tmpdir),'workdir')
    statefile = os.path.join(str(tmpdir),'state.json')
    result = runner.invoke(yadage.manualcli.init,[workdir,'workflow.yml','-t','testspecs/local-helloworld','-s','filebacked:'+statefile])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.preview,['-s','filebacked:'+statefile,'/hello_world'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.apply,['-s','filebacked:'+statefile,'-n','/hello_world'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.show,['-s','filebacked:'+statefile])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.visualize,['-s','filebacked:'+statefile,'-w',str(tmpdir)])
    assert result.exit_code == 0

    assert tmpdir.join('yadage_workflow_instance.pdf').check()

def test_submit():
    runner = CliRunner()
    result = runner.invoke(yadage.manualcli.submit)

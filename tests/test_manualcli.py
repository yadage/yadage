from click.testing import CliRunner
import yadage.manualcli
import os


def test_manual_helloworld(tmpdir):
    runner = CliRunner()
    workdir   = os.path.join(str(tmpdir),'workdir')
    statefile = os.path.join(str(tmpdir),'state.json')
    result = runner.invoke(yadage.manualcli.init,[workdir,'workflow.yml','-t','tests/testspecs/local-helloworld','-s','filebacked:'+statefile,'-p','par=value'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.preview,['-s','filebacked:'+statefile,'/init'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.apply_stage,['-s','filebacked:'+statefile,'/init'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.preview,['-s','filebacked:'+statefile,'/hello_world'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.apply_stage,['-s','filebacked:'+statefile,'/hello_world'])
    assert result.exit_code == 0

    #undo and redo
    result = runner.invoke(yadage.manualcli.undo_stage,['-s','filebacked:'+statefile,'/hello_world'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.apply_stage,['-s','filebacked:'+statefile,'/hello_world'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.show,['-s','filebacked:'+statefile])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.submit,['-s','filebacked:'+statefile,'-a','/init'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.visualize,['-s','filebacked:'+statefile,'-w',str(tmpdir)])
    assert result.exit_code == 0

    assert tmpdir.join('yadage_workflow_instance.pdf').check()

def test_manual_step(tmpdir):
    runner = CliRunner()
    workdir   = os.path.join(str(tmpdir),'workdir')
    statefile = os.path.join(str(tmpdir),'state.json')
    result = runner.invoke(yadage.manualcli.init,[workdir,'workflow.yml','-t','tests/testspecs/local-helloworld','-s','filebacked:'+statefile,'-p','par=value'])
    assert result.exit_code == 0

    result = runner.invoke(yadage.manualcli.step,['-s','filebacked:'+statefile, '-b','foregroundasync'])
    assert result.exit_code == 0


def test_manual_dynamicglob(tmpdir):
    runner = CliRunner()
    workdir   = os.path.join(str(tmpdir),'workdir')
    statefile = os.path.join(str(tmpdir),'state.json')
    result = runner.invoke(yadage.manualcli.init,[
        workdir,'workflow_frominit.yml',
        '-t','tests/testspecs/dynamic_glob',
        '-s','filebacked:'+statefile,
        '-d','inputarchive=file://{}/tests/testspecs/dynamic_glob/inputs/three_files.zip'.format(os.path.abspath(os.curdir)),
        '-p','inputfiles="*.txt"'
    ])
    assert result.exit_code == 0

def test_manual_add(tmpdir):
    workdir_one   = os.path.join(str(tmpdir),'workdir_one')
    workdir_two   = os.path.join(str(tmpdir),'workdir_two')

    runner = CliRunner()
    statefile = os.path.join(str(tmpdir),'state.json')
    result = runner.invoke(yadage.manualcli.init,[workdir_one,'workflow.yml','-t','tests/testspecs/local-helloworld','-s','filebacked:'+statefile,'-p','par=value'])
    result = runner.invoke(yadage.manualcli.add,[workdir_two,'workflow.yml','-t','tests/testspecs/mapreduce','-s','filebacked:'+statefile])

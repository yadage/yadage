from click.testing import CliRunner
import yadage.steering
import os

def test_maincli(tmpdir):
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','testspecs/local-helloworld'])


    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()
    assert result.exit_code == 0

def test_maincli_cmdline_pars(tmpdir):
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','testspecs/mapreduce','-p','input=[1,2,3]'])
    assert result.exit_code == 0


def test_maincli_initfile(tmpdir):
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','testspecs/mapreduce','testspecs/mapreduce/input.yml'])
    assert result.exit_code == 0

def test_maincli_interactive_all_yes(tmpdir):
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[
        os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','testspecs/local-helloworld','--interactive'], input = 'y\ny\n'
    )
    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()
    assert result.exit_code == 0


def test_maincli_interactive_no_yes(tmpdir):
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[
        os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','testspecs/local-helloworld','--interactive'], input = '\nn\ny\nn\ny\n'
    )
    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()
    assert result.exit_code == 0

def test_maincli_cached(tmpdir):
    cachefile = os.path.join(str(tmpdir),'cache.json')
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[
        os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','testspecs/local-helloworld','-c','checksums:'+cachefile]
    )
    assert result.exit_code == 0

    result = runner.invoke(yadage.steering.main,[
        os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','testspecs/local-helloworld','--accept-workdir','-c','checksums:'+cachefile]
    )

    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()
    assert result.exit_code == 0


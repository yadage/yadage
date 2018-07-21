from click.testing import CliRunner
import yadage.steering
import os

def test_maincli(tmpdir):
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','tests/testspecs/local-helloworld','-p','par=value'])


    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()
    assert result.exit_code == 0

def test_maincli_cmdline_pars(tmpdir):
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','tests/testspecs/mapreduce','-p','input=[1,2,3]'])
    assert result.exit_code == 0


def test_maincli_initfile(tmpdir):
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','tests/testspecs/mapreduce','tests/testspecs/mapreduce/input.yml'])
    assert result.exit_code == 0

def test_maincli_interactive_all_yes(tmpdir):
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[
        os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','tests/testspecs/local-helloworld','-g','interactive','-p','par=value'], input = 'y\ny\ny\ny\ny\ny\n'
    )
    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()
    assert result.exit_code == 0

def test_maincli_interactive_no_yes(tmpdir):
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[
        os.path.join(str(tmpdir),'workdir'),'workflow.yml','-t','tests/testspecs/local-helloworld','-g','interactive','-p','par=value'], input = 'y\ny\ny\ny\ny\ny\n'
    )
    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()
    assert result.exit_code == 0

def test_stackednested(tmpdir):
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,[os.path.join(str(tmpdir),'workdir'),
        'workflow.yml','-t','tests/testspecs/stackednestings','tests/testspecs/stackednestings/input.yml',
        '-d','initdir={}'.format(os.path.abspath('tests/testspecs/stackednestings/inputs'))
    ])
    assert result.exit_code == 0

def test_analysisscaffold(tmpdir):
    spec = '''\
dataarg: {workdir}
dataopts:
 subinits:
   signal: {signal}
   data: {data}
   backgrounds: {bkg}
workflow: workflow.yml
toplevel: {toplevel}
visualize: true
plugins: []
backend: 'foregroundasync'
backendopts: {{}}
'''.format(
        workdir = os.path.join(str(tmpdir),'workdir'),
        signal = os.path.abspath('tests/testspecs/analysis_scaffold/basedata/siginputs'),
        data = os.path.abspath('tests/testspecs/analysis_scaffold/basedata/datainputs'),
        bkg = os.path.abspath('tests/testspecs/analysis_scaffold/basedata/bkginputs'),
        toplevel = os.path.abspath('tests/testspecs/analysis_scaffold/workflow')
    )
    f = tmpdir.join('spec.yml')
    f.write(spec)
    runner = CliRunner()
    result = runner.invoke(yadage.steering.main,['-f',str(f)])
    assert result.exit_code == 0
    assert tmpdir.join('workdir/inference/stage1/output.txt').read()

from click.testing import CliRunner
import yadage.utilcli
import yadage.steering
import os

def test_testsel(tmpdir):
    runner = CliRunner()

    result = runner.invoke(yadage.steering.main,[str(tmpdir.join('workdir')),'workflow.yml','-t','testspecs/local-helloworld'])
    assert tmpdir.join('workdir/hello_world/hello_world.txt').check()


    result = runner.invoke(yadage.utilcli.testsel,[
		str(tmpdir.join('workdir/_yadage/yadage_snapshot_workflow.json')),
		str(tmpdir.join('workdir/_yadage/yadage_snapshot_backend.json')),
		'{stages: hello_world, output: outputfile}'
    ])
    assert result.exit_code == 0


def test_viz():
    runner = CliRunner()
    result = runner.invoke(yadage.utilcli.viz)

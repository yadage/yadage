from click.testing import CliRunner
import yadage.utilcli
import yadage.steering
import os


def test_clis(tmpdir):
    runner = CliRunner()

    result = runner.invoke(
        yadage.steering.main,
        [
            str(tmpdir.join("workdir")),
            "workflow.yml",
            "-t",
            "tests/testspecs/local-helloworld",
            "-p",
            "par=value",
            "-b",
            "foregroundasync",
        ],
    )
    assert tmpdir.join("workdir/hello_world/hello_world.txt").check()

    result = runner.invoke(
        yadage.utilcli.testsel,
        [
            str(tmpdir.join("workdir/_yadage/yadage_snapshot_workflow.json")),
            "{stages: hello_world, output: outputfile}",
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(
        yadage.utilcli.testsel,
        [
            str(tmpdir.join("workdir/_yadage/yadage_snapshot_workflow.json")),
            "{stages: nonexistent, output: nonexistent}",
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(
        yadage.utilcli.viz,
        [
            str(tmpdir.join("workdir/_yadage/yadage_snapshot_workflow.json")),
            str(tmpdir.join("viz.pdf")),
        ],
    )
    tmpdir.join("viz.pdf").check()
    assert result.exit_code == 0

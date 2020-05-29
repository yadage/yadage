import os
import yaml


def test_setup_filebacked(tmpdir):
    from yadage.state_providers.localposix import LocalFSProvider
    from packtivity.statecontexts.posixfs_context import LocalFSState
    import os

    localprovider = LocalFSProvider(
        LocalFSState(
            readwrite=[
                os.path.join(str(tmpdir), "rw_one"),
                os.path.join(str(tmpdir), "rw_two"),
            ],
            readonly=[
                os.path.join(str(tmpdir), "ro_one"),
                os.path.join(str(tmpdir), "ro_two"),
            ],
        ),
        init_states=[
            LocalFSState(
                readwrite=[
                    os.path.join(str(tmpdir), "init_rw_one"),
                    os.path.join(str(tmpdir), "init_rw_two"),
                ],
                readonly=[
                    os.path.join(str(tmpdir), "init_ro_one"),
                    os.path.join(str(tmpdir), "init_ro_two"),
                ],
            )
        ],
    )
    assert (
        LocalFSProvider.fromJSON(localprovider.json(), {}).json()
        == localprovider.json()
    )


def test_subinits():
    spec = """\
dataarg: analysisdata
dataopts:
 pathbase: /Users/lukas/Code/yadagedev/yadage/tests/testspecs/bsm_grid_scaffold/basedata
 subinits:
   signals:
     run_points_0: siginputs/sigpoint0
     run_points_1: siginputs/sigpoint1
     run_points_2: siginputs/sigpoint2
   data: datainputs
   backgrounds:
     run_bkgs_0: bkginputs/bkg_sample_0
     run_bkgs_1: bkginputs/bkg_sample_1
     run_bkgs_2: bkginputs/bkg_sample_2
     run_bkgs_3: bkginputs/bkg_sample_3
"""
    spec = yaml.safe_load(spec)
    from yadage.state_providers.localposix import setup_provider

    sp = setup_provider("local:" + spec["dataarg"], spec["dataopts"])
    assert type(sp).fromJSON(sp.json(), {}).json() == sp.json()

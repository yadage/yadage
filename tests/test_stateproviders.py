def test_setup_filebacked(tmpdir):
    from yadage.state_providers.localposix import LocalFSProvider
    from packtivity.statecontexts.posixfs_context import LocalFSState
    import os
    localprovider = LocalFSProvider(

        LocalFSState(
            readwrite = [
                os.path.join(str(tmpdir),'rw_one'),
                os.path.join(str(tmpdir),'rw_two')
            ],
            readonly = [
                os.path.join(str(tmpdir),'ro_one'),
                os.path.join(str(tmpdir),'ro_two')
            ]
        )
        ,
        init_states = [
            LocalFSState(
                    readwrite = [
                        os.path.join(str(tmpdir),'init_rw_one'),
                        os.path.join(str(tmpdir),'init_rw_two')
                    ],
                    readonly = [
                        os.path.join(str(tmpdir),'init_ro_one'),
                        os.path.join(str(tmpdir),'init_ro_two')
                    ]
            )
        ]
    )
    assert LocalFSProvider.fromJSON(localprovider.json(),{}).json() == localprovider.json()

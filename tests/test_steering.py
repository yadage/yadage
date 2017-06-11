from yadage.steering_object import YadageSteering
from yadage.clihelpers import setupbackend_fromstring

import logging
logging.basicConfig(level = logging.INFO)

def test_steer(tmpdir,multiproc_backend):
    ys = YadageSteering()
    ys.prepare_workdir(tmpdir.dirname)
    ys.init_workflow('workflow.yml', 'testspecs/nestedmapreduce',{'input': [1,2,3]})

    ys.adage_argument(default_trackers = False)
    ys.run_adage(multiproc_backend)

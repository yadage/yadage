import os
from yadage.steering_object import YadageSteering
from yadage.steering_api import steering_ctx

def test_steer(tmpdir,multiproc_backend):
    ys = YadageSteering()
    ys.prepare_workdir(os.path.join(str(tmpdir),'workdir'))
    ys.init_workflow('workflow.yml', 'testspecs/nestedmapreduce',{'input': [1,2,3]})

    ys.adage_argument(default_trackers = False)
    ys.run_adage(multiproc_backend)

def test_context(tmpdir,multiproc_backend):
	workdir = os.path.join(str(tmpdir),'workdir')
	with steering_ctx(workdir, 'workflow.yml', {'input':[1,2,3]}, 'testspecs/nestedmapreduce', multiproc_backend) as ys:
		ys.adage_argument(default_trackers = False)

from click.testing import CliRunner
from yadage.controllers import setup_controller_from_modelstring

def test_setup_filebacked(tmpdir):
	setup_controller_from_modelstring('filebacked:'+str(tmpdir.join('cache.json')))

def test_setup_inmem(tmpdir):
	setup_controller_from_modelstring('inmem')

import pytest
from yadage.wflownode import YadageNode
from yadage.tasks import packtivity_task


def test_create(basic_packtivity_spec,localfs_state):
	step = packtivity_task('myname',basic_packtivity_spec,localfs_state)
	node = YadageNode('myname',step,'identiifer')
	
def test_result_prepub(basic_packtivity_spec,localfs_state):
	step = packtivity_task('myname',basic_packtivity_spec,localfs_state)
	step.prepublished = {'hello': 'world'}
	node = YadageNode('myname',step,'identiifer')

	assert node.has_result() == True
	assert node.result == step.prepublished

	node.readfromresult('') == node.result
	node.readfromresult('/hello') == node.result['hello']


	another_step = packtivity_task('another',basic_packtivity_spec,localfs_state)
	node.readfromresult('/hello',another_step)
	assert another_step.inputs[-1].stepid == node.identifier
	assert another_step.inputs[-1].pointer.path == '/hello'

def test_serialize_desrialize(basic_packtivity_spec,localfs_state):
	data = {
		'task': {'type': 'init_task', 'name': 'aname', 'parameters': {},'prepublished':{},'inputs':[]},
		'name': '',
		'id': 'anid'
	}
	YadageNode.fromJSON(data)

	step = packtivity_task('myname',basic_packtivity_spec,localfs_state)
	packtivity_task.fromJSON(step.json()).json() == step.json()

def test_noresult(basic_packtivity_spec,localfs_state):
	step = packtivity_task('myname',basic_packtivity_spec,localfs_state)
	node = YadageNode('myname',step,'identiifer')
	assert node.has_result() == False
	node.readfromresult('', failsilently = True) == None
	with pytest.raises(RuntimeError):
		node.readfromresult('') == None


def test_repr(basic_packtivity_spec,localfs_state):
	step = packtivity_task('myname',basic_packtivity_spec,localfs_state)
	node = YadageNode('myname',step,'identiifer')
	assert repr(node)
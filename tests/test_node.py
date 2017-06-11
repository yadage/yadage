import pytest
from yadage.wflownode import YadageNode
from yadage.yadagestep import yadagestep
import packtivity.statecontexts.posixfs_context as statecontext

def test_create():
	spec = {}
	ctx  = statecontext.make_new_context('/workdir')
	step = yadagestep('myname',spec,ctx)
	node = YadageNode('myname',step,'identiifer')
	
def test_result_prepub():
	spec = {}
	ctx  = statecontext.make_new_context('/workdir')
	step = yadagestep('myname',spec,ctx)
	step.prepublished = {'hello': 'world'}
	node = YadageNode('myname',step,'identiifer')

	assert node.has_result() == True
	assert node.result == step.prepublished

	node.readfromresult('') == node.result
	node.readfromresult('/hello') == node.result['hello']


	another_step = yadagestep('another',spec,ctx)
	node.readfromresult('/hello',another_step)
	assert another_step.inputs[-1].stepid == node.identifier
	assert another_step.inputs[-1].pointer.path == '/hello'

def test_serialize_desrialize():
	data = {
		'task': {'type': 'initstep', 'name': 'aname', 'attributes': {},'prepublished':{},'inputs':[]},
		'name': '',
		'id': 'anid'
	}
	YadageNode.fromJSON(data)

	data = {
		'task': {'type': 'yadagestep', 'name': 'aname','spec': {}, 'context': {}, 'attributes': {},'prepublished':{},'inputs':[]},
		'name': '',
		'id': 'anid'
	}
	YadageNode.fromJSON(data)

def test_noresult():
	spec = {}
	ctx  = statecontext.make_new_context('/workdir')
	step = yadagestep('myname',spec,ctx)
	node = YadageNode('myname',step,'identiifer')
	assert node.has_result() == False
	node.readfromresult('', failsilently = True) == None
	with pytest.raises(RuntimeError):
		node.readfromresult('') == None


def test_repr():
	spec = {}
	ctx  = statecontext.make_new_context('/workdir')
	step = yadagestep('myname',spec,ctx)
	node = YadageNode('myname',step,'identiifer')
	assert repr(node)
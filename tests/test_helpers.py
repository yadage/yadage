import yadage.helpers

def test_leafit():
	data = {'hello': 'world', 'list': [1,2,3], 'nested':{'dict':['like','structure']}}
	assert len(list(yadage.helpers.leaf_iterator(data))) == 6

	assert set((x.path,y) for x,y in yadage.helpers.leaf_iterator(data)) == {
		('/hello','world'),
		('/list/0',1),
		('/list/1',2),
		('/list/2',3),
		('/nested/dict/0','like'),
		('/nested/dict/1','structure'),
	}


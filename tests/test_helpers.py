import yadage.helpers
import yadage.clihelpers
import os

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

def test_filediscovery(tmpdir):
	tmpdir.join('afile').ensure()
	tmpdir.join('bfile1').ensure()
	tmpdir.join('bfile2').ensure()
	tmpdir.join('cfile').ensure()
	initdata = {'key1':'afile','key2':'bfile*','key3':'nofile','nested':{'file':'cfile'}}
	data = yadage.clihelpers.discover_initfiles(initdata,str(tmpdir))
	assert data['key1'] == os.path.join(str(tmpdir),'afile')
	assert set(data['key2']) == set([os.path.join(str(tmpdir),'bfile1'),os.path.join(str(tmpdir),'bfile2')])
	assert data['key3'] == 'nofile'
	assert data['nested']['file'] == os.path.join(str(tmpdir),'cfile')


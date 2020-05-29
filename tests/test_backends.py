from yadage.utils import setupbackend_fromstring
from yadage.backends.packtivitybackend import PacktivityBackend


def test_mytest():
    backend = setupbackend_fromstring("multiproc:4")
    assert type(backend) == PacktivityBackend

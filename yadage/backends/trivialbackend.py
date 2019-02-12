from packtivity.asyncbackends import PacktivityProxyBase
from packtivity import datamodel as _datamodel

class TrivialProxy(PacktivityProxyBase):
    '''
    A trivial proxy that carries the results and status already
    '''

    def __init__(self, status, resultdata, datamodel):
        self.status = status
        self.resultdata = resultdata
        self.datamodel = datamodel

    def proxyname(self):
        return 'TrivialProxy'

    def details(self):
        return {
            'resultdata':self.resultdata,
            'datamodel': self.datamodel,
            'status':self.status
        }

    @classmethod
    def fromJSON(cls,data):
        return cls(**data['proxydetails'])

class TrivialBackend(object):
    '''
    A trivial backend that can only return proxy based information, no submission
    '''

    def __init__(self):
        self.datamodel = _datamodel

    def submit(self, task):
        raise NotImplementedError(
            'The trivial proxy is not made for submission')

    def result(self, resultproxy):
        return self.datamodel.create(resultproxy.resultdata, resultproxy.datamodel)

    def expected_result(self, resultproxy):
        return None

    def ready(self, resultproxy):
        # when we have a proxy it is by definition ready...
        return True

    def successful(self, resultproxy):
        return resultproxy.status == 'SUCCESS'

    def fail_info(self, resultproxy):
        return None

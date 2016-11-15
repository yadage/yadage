from packtivitybackend import PacktivityProxyBase

class CERNReuseBackendProxy(PacktivityProxyBase):
    '''
    A proxy to a job submitted to the CERN (still unnamed) reuse backend
    '''
    def __init__(self):
        pass

    def details(self):
        return None
        # return {
        #     'task_id': self.proxy.task_id,
        # }

    @classmethod
    def fromJSON(cls, data):
        # proxy = AsyncResult(
        #     data['proxydetails']['task_id'],
        #     task_name=data['proxydetails']['task_name']
        # )
        return cls()

class CERNReuseBackend(object):
    '''
    The CERN (still unnamed) reuse backend
    '''
    def submit(self, task):
        #this needs to return CERNReuseBackendProxy instances
        raise NotImplementedError('wait for it...')

    def result(self, resultproxy):
        raise NotImplementedError('wait for it...')

    def ready(self, resultproxy):
        raise NotImplementedError('wait for it...')

    def successful(self, resultproxy):
        raise NotImplementedError('wait for it...')

    def fail_info(self, resultproxy):
        raise NotImplementedError('wait for it...')

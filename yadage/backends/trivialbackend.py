class TrivialProxy(object):
    '''
    A trivial proxy that carries the results and status already
    '''

    def __init__(self, status, result):
        self.status = status
        self.result = result

    def json(self):
        return None


class TrivialBackend(object):
    '''
    A trivial backend that can only return proxy based information, no submission
    '''

    def submit(self, task):
        raise NotImplementedError(
            'The trivial proxy is not made for submission')

    def result(self, resultproxy):
        return resultproxy.result

    def ready(self, resultproxy):
        # when we have a proxy it is by definition ready...
        return True

    def successful(self, resultproxy):
        return resultproxy.status == 'SUCCESS'

    def fail_info(self, resultproxy):
        return None

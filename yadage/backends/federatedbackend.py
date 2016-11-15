class FederatedBackend(object):
    '''
    A meta backend that routes tasks to several internal backends
    '''

    def __init__(self, backends):
        '''takes a dictionary of backendname->backendobject and a router'''
        self.backends = backends

    def routedsubmit(self, task):
        raise NotImplementedError('needs implementation')

    def routeproxy(self, proxy):
        raise NotImplementedError('needs implementation')

    def submit(self, task):
        return self.routedsubmit(task)

    def result(self, proxy):
        b = self.routeproxy(proxy)
        return self.backends[b].result(proxy)

    def ready(self, proxy):
        b = self.routeproxy(proxy)
        return self.backends[b].ready(proxy)

    def successful(self, proxy):
        b = self.routeproxy(proxy)
        return self.backends[b].successful(proxy)

    def fail_info(self, proxy):
        b = self.routeproxy(proxy)
        return self.backends[b].fail_info(proxy)

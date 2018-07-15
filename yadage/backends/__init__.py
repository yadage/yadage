from packtivity.backendutils import proxyloader, load_proxy
from yadage.backends.trivialbackend import TrivialProxy

class CachedProxy(object):
    def __init__(self,proxy,cacheid):
        self.proxy = proxy
        self.cacheid = cacheid

    def details(self):
        return {}

    def json(self):
        return {'proxyname': 'CachedProxy', 'proxy': self.proxy.json(), 'cacheid': self.cacheid}

    @classmethod
    def fromJSON(cls,data,deserialization_opts = None):
        deserialization_opts = deserialization_opts or {}
        return cls(load_proxy(data['proxy'],deserialization_opts), data['cacheid'],best_effort_backend = False)

@proxyloader('CachedProxy')
def cache_loader(jsondata, deserialization_opts = None, best_effort_backend = False):
    proxy = CachedProxy.fromJSON(jsondata,deserialization_opts)
    if best_effort_backend:
        raise NotImplementedError('nope')
    return proxy

@proxyloader('TrivialProxy')
def trivial_loader(jsondata, deserialization_opts = None, best_effort_backend = False):
    proxy = TrivialProxy.fromJSON(jsondata)
    if best_effort_backend:
        raise NotImplementedError('nope')
    return proxy

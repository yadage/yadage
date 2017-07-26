class CachedProxy(object):
    def __init__(self,proxy,cacheid):
        self.proxy = proxy
        self.cacheid = cacheid

    def json(self):
        return {'proxyname': 'CachedProxy', 'proxy': self.proxy.json(), 'cacheid': self.cacheid}

    @classmethod
    def fromJSON(cls,data):
        return cls(load_proxy(data['proxy']), data['cacheid'])

def load_proxy(data):
        import packtivity.asyncbackends
        import yadage.backends.packtivitybackend
        import yadage.backends.trivialbackend
        if data['proxyname']=='InitProxy':
            return yadage.backends.packtivitybackend.InitProxy.fromJSON(data)
        elif data['proxyname']=='CeleryProxy':
            return packtivity.asyncbackends.CeleryProxy.fromJSON(data)
        elif data['proxyname']=='ForegroundProxy':
            return packtivity.asyncbackends.ForegroundProxy.fromJSON(data)
        elif data['proxyname']=='CachedProxy':
            return CachedProxy.fromJSON(data)
        elif data['proxyname']=='TrivialProxy':
            return yadage.backends.trivialbackend.TrivialProxy.fromJSON(data)
        else:
            raise RuntimeError('unknown proxy found with name: {}'.format(data['proxyname']))

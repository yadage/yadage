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
        import packtivity.backendutils
        import yadage.backends.packtivitybackend
        import yadage.backends.trivialbackend


        ## test if this is one of the yadage-specfic proxies
        if data['proxyname']=='InitProxy':
            return yadage.backends.packtivitybackend.InitProxy.fromJSON(data)
        elif data['proxyname']=='CachedProxy':
            return CachedProxy.fromJSON(data)
        elif data['proxyname']=='TrivialProxy':
            return yadage.backends.trivialbackend.TrivialProxy.fromJSON(data)
        from_pack_proxy = packtivity.backendutils.proxy_from_json(data,best_effort_backend = False)
        if from_pack_proxy:
            return from_pack_proxy
        else:
            raise RuntimeError('unknown proxy found with name: {}'.format(data['proxyname']))

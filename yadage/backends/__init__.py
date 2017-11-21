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
        return cls(load_proxy(data['proxy'],deserialization_opts), data['cacheid'])

def load_proxy(data,deserialization_opts = None):
    deserialization_opts = deserialization_opts or {}
    import packtivity.backendutils
    import yadage.backends.packtivitybackend
    import yadage.backends.trivialbackend


    ## test if this is one of the yadage-specfic proxies
    if data['proxyname']=='CachedProxy':
        return CachedProxy.fromJSON(data,deserialization_opts)
    elif data['proxyname']=='TrivialProxy':
        return yadage.backends.trivialbackend.TrivialProxy.fromJSON(data)

    # it must be a packtivity proxy
    from_pack_proxy = packtivity.backendutils.load_proxy(
        data,
        deserialization_opts = deserialization_opts,
        best_effort_backend = False
    )
    if from_pack_proxy:
        return from_pack_proxy
    else:
        raise RuntimeError('unknown proxy found with name: {}'.format(data['proxyname']))

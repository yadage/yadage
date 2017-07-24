def load_proxy(data):
        import packtivity.asyncbackends
        import yadage.backends.packtivitybackend
        if data['proxyname']=='InitProxy':
            return yadage.backends.packtivitybackend.InitProxy.fromJSON(data)
        elif data['proxyname']=='CeleryProxy':
            return packtivity.asyncbackends.CeleryProxy.fromJSON(data)
        else:
            raise RuntimeError('only celery support for now... found proxy with name: {}'.format(data['proxyname']))

from packtivity.asyncbackends import ForegroundProxy, ForegroundBackend
import logging

log = logging.getLogger(__name__)

class InitProxy(ForegroundProxy):
    def __init__(self, *args, **kwargs):
        super(InitProxy,self).__init__(*args, **kwargs)
    def proxyname(self):
        return 'InitProxy'

class InitBackend(ForegroundBackend):
    def submit(self, spec, parameters, state, metadata):
        foreground_proxy = super(InitBackend,self).submit(spec, parameters, state,metadata)
        return InitProxy(foreground_proxy.result, foreground_proxy.success)

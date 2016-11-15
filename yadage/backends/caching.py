import federatedbackend
import json
from yadage.helpers import get_obj_id
from packtivitybackend import PacktivityTrivialProxy

import logging
log = logging.getLogger(__name__)

class CachedBackend(federatedbackend.FederatedBackend):
    def __init__(self,backend,cacheconfig = None):
        super(CachedBackend,self).__init__({
            'cache':TrivialBackend(),
            'primary':backend
        })
        cachefile = cacheconfig
        self.cache = json.load(open(cachefile)) if cachefile else None

    def cachevalid(self,task):
        return True #ToDo :)

    def cacheddata(self,task):
        hashid = get_obj_id(task)
        if not self.cache: return None
        if not self.cachevalid(task): return None
        return self.cache.get(hashid,None)

    def routedsubmit(self,task):
        cached = self.cacheddata(task)
        if cached:
            log.info('use cached result for task: {}'.format(task))
            return PacktivityTrivialProxy(**cached)
        else:
            return self.backends['primary'].submit(task)

    def routeproxy(self,proxy):
        if type(proxy) == PacktivityTrivialProxy:
            return 'cache'
        else:
            return 'primary'

class TrivialBackend(object):
    def result(self,resultproxy):
        return resultproxy.result

    def ready(self,resultproxy):
        return True

    def successful(self,resultproxy):
        return resultproxy.status == 'SUCCESS'

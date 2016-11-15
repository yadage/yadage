import federatedbackend
import json
import time
from yadage.helpers import get_obj_id
from trivialbackend import TrivialProxy, TrivialBackend

import logging
log = logging.getLogger(__name__)


class CachedBackend(federatedbackend.FederatedBackend):
    '''
    A caching backend that takes an existing backend and a cache configuration.
    If at submission time a cache for a given task is valid, the result will be
    returned directly via a trivial proxy. If not submission proceeds as normal
    '''

    def __init__(self, backend, cacheconfig=None):
        super(CachedBackend, self).__init__({
            'cache': TrivialBackend(),
            'primary': backend
        })
        self.cache = CacheBuilder(cacheconfig)

    def ready(self, proxy):
        isready = super(CachedBackend, self).ready(proxy)
        if isready and type(proxy) is not TrivialProxy:
            result = super(CachedBackend, self).result(proxy)
            status = super(CachedBackend, self).successful(proxy)
            self.cache.cacheresult(proxy.taskhash, status, result)
        return isready

    def routedsubmit(self, task):
        cached = self.cache.cacheddata(task)
        if cached:
            log.info('use cached result for task: {}'.format(task))
            return TrivialProxy(status=cached['status'], result=cached['result'])
        else:
            hashid = get_obj_id(task)
            primaryproxy = self.backends['primary'].submit(task)
            primaryproxy.taskhash = hashid
            return primaryproxy

    def routeproxy(self, proxy):
        if type(proxy) == TrivialProxy:
            return 'cache'
        else:
            return 'primary'


class CacheBuilder(object):

    def __init__(self, cachefile):
        self.cachefile = cachefile

    def cacheresult(self, taskhash, status, result):
        cache = json.load(open(self.cachefile))
        cache[taskhash] = {
            'status': 'SUCCESS' if status else 'FAILED',
            'result': result,
            'cachingtime': time.time()
        }
        json.dump(cache, open(self.cachefile, 'w'))

    def cachevalid(self, task):
        return True #TODO

    def cacheddata(self, task):
        hashid = get_obj_id(task)
        if not self.cachevalid(task):
            return None
        cache = json.load(open(self.cachefile))
        return cache.get(hashid, None)

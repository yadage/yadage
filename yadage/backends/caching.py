import json
import logging
import os
import time

import yadage.backends.federatedbackend as federatedbackend
from packtivity import datamodel as _datamodel

from packtivity.statecontexts import load_state
from yadage.utils import json_hash

from ..backends import CachedProxy
from .trivialbackend import TrivialBackend, TrivialProxy

log = logging.getLogger(__name__)

from ..handlers.utils import handler_decorator

cachehandlers, cachestrategy = handler_decorator()

@cachestrategy('checksums')
def checksum_cache(config):
    configfile = config
    return ChecksumCache(configfile)


def setupcache_fromstring(configstring):
    '''
    generate cache from a string configuration (such as those passed from CLIs).
    valid strings are:

    - checksums:<path to cache file (JSON)>

    :param configstring: the configuration string
    :return: a cache object
    '''
    configparts  = configstring.split(':',1)
    strategy, specificconf = configparts
    try:
        return cachehandlers[strategy](specificconf)
    except KeyError:
        raise RuntimeError('unknown caching config')

class CachedBackend(federatedbackend.FederatedBackend):
    '''
    A caching backend that takes an existing backend and a cache configuration.
    If at submission time a cache for a given task is valid, the result will be
    returned directly via a trivial proxy. If not submission proceeds as normal
    '''

    def __init__(self, backend, cache):
        super(CachedBackend, self).__init__({
            'cache': TrivialBackend(),
            'primary': backend
        })
        self.cache = cache
        self.primary_enabled = True

    def ready(self, proxy):
        isready = super(CachedBackend, self).ready(proxy)
        #if this a genuinely new result, cache it under the task cache id
        if isready and type(proxy) is not TrivialProxy:
            result = super(CachedBackend, self).result(proxy)
            status = super(CachedBackend, self).successful(proxy)
            if not self.cache.cacheexists(proxy.cacheid):
                self.cache.cacheresult(proxy.cacheid, status, result)
        return isready

    def expected_result(self, task):
        return self.backends['packtivity'].prepublish(task)

    def routedsubmit(self, task):
        cached = self.cache.cacheddata(task)
        if cached:
            log.info('use cached result for task: %s',task.metadata['name'])
            return TrivialProxy(cached['status'], cached['result'].json(), task.state.datamodel)
        else:
            if not self.primary_enabled:
                raise RuntimeError('cache failed but refusing to submit to primary since it was explicitly disabled')
            log.info('do proper submit for task: %s',task.metadata['name'])
            #create id for this task using the cache builder, with which
            #we will store the result with once it's ready
            cacheid = self.cache.cacheid(task)

            task.state.reset()
            #maybe we erroneously are re-submitting this see
            #https://github.com/diana-hep/yadage/issues/45 TODO!
            primaryproxy = self.backends['primary'].submit(task.spec, task.parameters, task.state, task.metadata)
            cachedproxy  = CachedProxy(primaryproxy, cacheid)
            return cachedproxy

    def routeproxy(self, proxy):
        if type(proxy) == TrivialProxy:
            return 'cache', proxy
        else:
            return 'primary', proxy.proxy


class CacheBuilder(object):
    '''
    Cache base class. Implementations need to provide cachevalid and
    generate_validation_data methods
    '''
    def __init__(self, cachefile):
        self.datamodel = _datamodel
        self.cachefile = cachefile
        if not os.path.exists(self.cachefile):
            log.info('fresh cache as file at %s does not exist yet.',self.cachefile)
            self.cache = {}
        else:
            log.info('reading cache from %s',self.cachefile)
            self.cache = json.load(open(self.cachefile))

    def todisk(self):
        log.info('writing cache to %s',self.cachefile)
        json.dump(self.cache, open(self.cachefile, 'w'), indent=4, sort_keys=True)

    def remove(self,cacheid):
        self.cache.pop(cacheid)

    def cacheresult(self, cacheid, status, result):
        '''
        saves a result and its status under a unique identifier and includes
        validation data to verify the cache validity at a later point in time
        '''
        log.debug('caching result for cacheid: %s',cacheid)
        log.debug('caching result for process: %s',self.cache[cacheid]['task']['spec']['process'])
        self.cache[cacheid]['result'] = {
            'status': 'SUCCESS' if status else 'FAILED',
            'result': result.json(),
            'cachingtime': time.time(),
            'validation_data': self.generate_validation_data(cacheid)
        }
        self.todisk()

    def cachedresult(self,cacheid, state, silent = True):
        '''
        returns the cached result. when silent = True the mthod exits gracefully
        and returns None
        '''
        if silent:
            if not self.cacheexists(cacheid): return None
        return {
            'result': self.datamodel.create(self.cache[cacheid]['result']['result'], state.datamodel),
            'status': self.cache[cacheid]['result']['status']
        }

    def cacheid(self,task):
        t = task.json()
        tocache = [t['state'], t['parameters'], t['spec']]
        hash = json_hash(tocache)
        # log.info('%s -> %s', json.dumps(tocache), hash)
        return hash

    def cacheexists(self,cacheid):
        return cacheid in self.cache and 'result' in self.cache[cacheid]

    def cacheddata(self, task, remove_invalid = True):
        '''
        returns results from a valid cache entry if it exists, else None
        if remove_invalid = True, also removes invalid cache entries from cache
        '''
        cacheid = self.cacheid(task)
        #register this task with the cacheid if we don't know about it yet
        log.debug('checking cache for task %s',task.metadata['name'], cacheid)
        if cacheid not in self.cache:
            self.cache[cacheid] = {'task' : task.json()}
        if not self.cacheexists(cacheid):
            log.info('cache non-existent for task %s (%s)',task.metadata['name'],cacheid)
            return None
        if not self.cachevalid(cacheid):
            self.remove(cacheid)
            self.cache[cacheid] = {'task' : task.json()}
            return None
        #return a cached result if we have one if not, return None
        result =  self.cachedresult(cacheid, task.state, silent = True)
        log.debug('returning cached result %s',result)
        return result

    def cachevalid(self, cacheid):
        raise NotImplementedError

    def generate_validation_data(self,cacheid):
        raise NotImplementedError

class ChecksumCache(CacheBuilder):
    '''
    This is a cache that based on state hashes
    '''

    def __init__(self, cachefile):
        super(ChecksumCache, self).__init__(cachefile)

    def remove(self,cacheid):
        '''
        removes entry from the cache and reset its state

        :param cachid: the cache entry identifier
        '''
        task = self.cache[cacheid]['task']
        log.debug('removing cache entry %s and resetting state',cacheid)
        load_state(task['state']).reset()
        super(ChecksumCache, self).remove(cacheid)


    def generate_validation_data(self,cacheid):
        '''
        calculate validation data (checksums in SHA1 hash form)

        :param cachid: the cache entry identifier
        '''
        validation_data = {
            'state_hash': load_state(self.cache[cacheid]['task']['state']).state_hash()
        }

        log.debug('validation data is %s',validation_data)
        return validation_data

    def cachevalid(self, cacheid):
        '''
        validate cache entry

        :param cachid: the cache entry identifier
        '''
        # task = self.cache[cacheid]['task']

        stored_validation_data = self.cache[cacheid]['result']['validation_data']
        validation_data_now = self.generate_validation_data(cacheid)

        if not stored_validation_data == validation_data_now:
            log.info('cache invalid')
            return False
        log.info('cache valid')
        return True

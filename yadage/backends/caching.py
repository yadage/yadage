import federatedbackend
import json
import time
import os
import logging
import jq
import jsonpointer
import checksumdir
import shutil

from yadage.helpers import get_obj_id
from trivialbackend import TrivialProxy, TrivialBackend

log = logging.getLogger(__name__)

class CachedBackend(federatedbackend.FederatedBackend):
    '''
    A caching backend that takes an existing backend and a cache configuration.
    If at submission time a cache for a given task is valid, the result will be
    returned directly via a trivial proxy. If not submission proceeds as normal
    '''

    def __init__(self, backend, cacheconfig):
        super(CachedBackend, self).__init__({
            'cache': TrivialBackend(),
            'primary': backend
        })
        configparts  = cacheconfig.split(':')
        if len(configparts) == 1:
            log.info('default caching strategy')
            self.cache = CacheBuilder(configparts[0])
        else:
            strategy, configfile = configparts
            if strategy == 'checksums':
                log.info('checksums caching strategy')
                self.cache = ChecksumCache(configfile)
            else:
                raise RuntimeError('unknown caching config')


    def ready(self, proxy):
        isready = super(CachedBackend, self).ready(proxy)
        #if this a genuinely new result, cache it under the task cache id
        if isready and type(proxy) is not TrivialProxy:
            result = super(CachedBackend, self).result(proxy)
            status = super(CachedBackend, self).successful(proxy)
            if not self.cache.cacheexists(proxy.cacheid):
                self.cache.cacheresult(proxy.cacheid, status, result)
        return isready

    def routedsubmit(self, task):
        cached = self.cache.cacheddata(task)
        if cached:
            log.info('use cached result for task: %s',task.name)
            return TrivialProxy(status=cached['status'], result = cached['result'])
        else:
            log.info('do proper submit for task: %s',task.name)
            #create id for this task using the cache builder, with which
            #we will store the result with once it's ready
            cacheid = self.cache.cacheid(task)
            primaryproxy = self.backends['primary'].submit(task.spec, task.attributes, task.context)
            primaryproxy.cacheid = cacheid
            return primaryproxy

    def routeproxy(self, proxy):
        if type(proxy) == TrivialProxy:
            return 'cache'
        else:
            return 'primary'


class CacheBuilder(object):
    def __init__(self, cachefile):
        self.cachefile = cachefile
        if not os.path.exists(self.cachefile):
            self.cache = {}
        else:
            log.info('reading cache from %s',self.cachefile)
            self.cache = json.load(open(self.cachefile))

    def __del__(self):
        log.info('writing cache to %s',self.cachefile)
        json.dump(self.cache, open(self.cachefile, 'w'), indent=4, sort_keys=True)

    def remove(self,cacheid):
        self.cache.pop(cacheid)


    def generate_validation_data(self,cacheid):
        raise NotImplementedError


    def cacheresult(self, cacheid, status, result):
        '''
        saves a result and its status under a unique identifier and includes
        validation data to verify the cache validity at a later point in time
        '''
        log.info('caching result for cacheid: %s',cacheid)
        log.info('caching result for process: %s',self.cache[cacheid]['task']['spec']['process'])
        self.cache[cacheid]['result'] = {
            'status': 'SUCCESS' if status else 'FAILED',
            'result': result,
            'cachingtime': time.time(),
            'validation_data': self.generate_validation_data(cacheid)
        }

    def cachedresult(self,cacheid, silent = True):
        '''
        returns the cached result. when silent = True the mthod exits gracefully
        and returns None
        '''
        if silent:
            if not self.cacheexists(cacheid): return None
        return self.cache[cacheid]['result']

    def cacheid(self,task):
        return get_obj_id(task, method = 'jsonhash')

    def cacheexists(self,cacheid):
        return cacheid in self.cache and 'result' in self.cache[cacheid]

    def cachevalid(self, cacheid):
        raise RuntimeError('implement cache validation')


    def cacheddata(self, task, remove_invalid = True):
        '''
        returns results from a valid cache entry if it exists, else None
        if remove_invalid = True, also removes invalid cache entries from cache
        '''
        cacheid = self.cacheid(task)
        #register this task with the cacheid if we don't know about it yet
        log.info('checking cache for task %s',task.name)
        if cacheid not in self.cache:
            self.cache[cacheid] = {'task' : task.json()}
        if not self.cacheexists(cacheid):
            log.info('cache non-existent for task %s (%s)',task.name,cacheid)
            return None
        if not self.cachevalid(cacheid):
            log.info('cache non-valid for task %s (%s)',task.name,cacheid)
            self.remove(cacheid)
            self.cache[cacheid] = {'task' : task.json()}
            return None
        #return a cached result if we have one if not, return None
        result =  self.cachedresult(cacheid, silent = True)
        log.info('returning cached result %s',result)
        return result

class ChecksumCache(CacheBuilder):
    '''
    This is a cache that attempts checks for file paths in result object
    leaves and checks if they already exist. If yes
    '''
    def __init__(self, cachefile):
        super(ChecksumCache, self).__init__(cachefile)

    def remove(self,cacheid):
        task = self.cache[cacheid]['task']
        log.info('removing cache entry %s',cacheid)
        workdir = task['context']['readwrite'][0]
        log.info('deleting rw location %s',workdir)
        if os.path.exists(workdir):
            shutil.rmtree(workdir)
        super(ChecksumCache, self).remove(cacheid)


    def generate_validation_data(self,cacheid):
        validation_data = {}

        log.info('compute dep checksums for %s',self.cache[cacheid]['task']['context']['depwrites'])
        dep_checksums = [checksumdir.dirhash(d) for d in self.cache[cacheid]['task']['context']['depwrites'] if os.path.isdir(d)]

        log.info('compute checksums for %s',self.cache[cacheid]['task']['context']['readwrite'])
        state_checksums = [checksumdir.dirhash(d) for d in self.cache[cacheid]['task']['context']['readwrite'] if os.path.isdir(d)]

        validation_data = {
            'depstate_checksums': dep_checksums,
            'state_checksums': state_checksums
        }

        log.info('validation data is are %s',validation_data)
        return validation_data


    def cachevalid(self, cacheid):
        task = self.cache[cacheid]['task']
        result = self.cache[cacheid]['result']['result']
        if task['type'] == 'initstep':
            return True


        #check if dependent state is still the same

        stored_validation_data = self.cache[cacheid]['result']['validation_data']
        validation_data_now = self.generate_validation_data(cacheid)


        valid_depstate = (validation_data_now['depstate_checksums'] == stored_validation_data['depstate_checksums'])
        valid_state = (validation_data_now['state_checksums'] == stored_validation_data['state_checksums'])
        log.info('checksums comparison: %s',validation_data_now == stored_validation_data)
        if not valid_depstate:
            log.info('cache invalid due to changed input state')
            return False
        if not valid_state:
            log.info('cache invalid due to changed data in output state')
            return False

        log.info('cache valid')
        return True

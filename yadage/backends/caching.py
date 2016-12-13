import federatedbackend
import json
import time
import os
import logging
import jq
import jsonpointer
import checksumdir

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
            if strategy == 'resultexists':
                log.info('resultexists caching strategy')
                self.cache = ResultFilesExistCache(configfile)
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
            log.info('use cached result for task: {}'.format(task.name))
            return TrivialProxy(status=cached['status'], result = cached['result'])
        else:
            log.info('do proper submit for task: {}'.format(task.name))
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

    def cacheresult(self, cacheid, status, result):
        log.info('caching result for cacheid: {}'.format(cacheid))
        log.info('caching result for process: {}'.format(self.cache[cacheid]['task']['spec']['process']))
        self.cache[cacheid]['result'] = {
            'status': 'SUCCESS' if status else 'FAILED',
            'result': result,
            'cachingtime': time.time(),
            'checksums': None
        }
        if 'context' in self.cache[cacheid]['task']:
            checksums = [checksumdir.dirhash(d) for d in self.cache[cacheid]['task']['context']['depwrites']]
            log.info('compute checksums for %s',self.cache[cacheid]['task']['context']['depwrites'])
            log.info('checksums are %s',checksums)
            self.cache[cacheid]['result']['checksums'] = checksums

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
        return True

    def cacheddata(self, task, remove_invalid = True):
        '''
        returns results from a valid cache entry if it exists, else None
        if remove_invalid = True, also removes invalid cache entries from cache
        '''
        cacheid = self.cacheid(task)
        #register this task with the cacheid if we don't know about it yet
        log.info('checking cache for task %s',task.name)
        if not cacheid in self.cache:
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

class ResultFilesExistCache(CacheBuilder):
    '''
    This is a cache that attempts checks for file paths in result object
    leaves and checks if they already exist. If yes
    '''
    def __init__(self, cachefile):
        super(ResultFilesExistCache, self).__init__(cachefile)

    def remove(self,cacheid):
        import shutil
        task = self.cache[cacheid]['task']
        log.info('removing cache entry %s',cacheid)
        workdir = task['context']['readwrite'][0]
        log.info('deleting rw location %s',workdir)
        if os.path.exists(workdir):
            shutil.rmtree(workdir)
        super(ResultFilesExistCache, self).remove(cacheid)

    def cachevalid(self, cacheid):
        task = self.cache[cacheid]['task']
        result = self.cache[cacheid]['result']['result']
        if task['type'] == 'initstep':
            return True


        #check if dependent state is still the same

        stored_checksums = self.cache[cacheid]['result']['checksums']
        checksums_now = [checksumdir.dirhash(d) for d in task['context']['depwrites']]
        log.info('checksums comparison: %s',checksums_now == stored_checksums)
        if not checksums_now == stored_checksums:
            log.info('cache invalid due to changed input state')
            log.info(task['context']['depwrites'])
            log.info(stored_checksums)
            log.info(checksums_now)
            return False

        #check if our result fragments are still there
        rwloc = task['context']['readwrite'][0]
        resultleafs = jq.jq('leaf_paths').transform(result, multiple_output=True)
        resultleafs = [jsonpointer.JsonPointer.from_parts(x).resolve(result) for x in resultleafs]
        for x in resultleafs:
            if rwloc in x:
                exists = os.path.exists(x)
                if not exists:
                    log.info('cache invalid due to missing data fragments')
                    return False
        log.info('all file refs in result exist already.')
        return True

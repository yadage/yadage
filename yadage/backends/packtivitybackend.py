import logging
from packtivity.backendutils import backend_from_string

import yadage.backends.caching as caching
import yadage.backends.federatedbackend as federatedbackend
from ..tasks import packtivity_task, init_task
from .trivialbackend import TrivialProxy, TrivialBackend

log = logging.getLogger(__name__)


class InitProxy(TrivialProxy):
    def proxyname(self):
        return 'InitProxy'


class PacktivityBackend(federatedbackend.FederatedBackend):
    '''
    a backend that mainly submits step tasks to a packtivity backend
    except for init nodes, which are resolvable trivially.
    '''

    def __init__(self,  packtivity_backendstring = None, packtivity_backend = None):
        if packtivity_backendstring:
            is_sync, backend = backend_from_string(packtivity_backendstring)
            assert not is_sync
        elif packtivity_backend:
            backend = packtivity_backend
        else:
            raise RuntimeError('need backend or backendstring')
        self.cached = False
        super(PacktivityBackend, self).__init__({
            'init': TrivialBackend(),
            'packtivity': backend
        })

    def enable_cache(self,cachestring = None, cache = None):
        if not (cache or cachestring):
            raise RuntimeError('need to provide either cache object or cache config string')

        self.cached = True
        self.backends['packtivity'] = caching.CachedBackend(
            self.backends['packtivity'],
            cache = cache if cache else caching.setupcache_fromstring(cachestring)
        )

    def routedsubmit(self, task):
        tasktype = type(task)
        if tasktype == packtivity_task:
            #this is a little hacky, because the packtivity backends
            #take unrolled spec/parameters/context while the adage API
            #takes generalized task objects
            #possibly could use Munch on the packtivity side to
            #dynammicaly create .task/.parameters/.state-able objects
            if self.cached:
                #Cached backends adhere to the task-based API
                return self.backends['packtivity'].submit(task)
            else:
                #primary packtivity backends adhere to the unrolled API
                return self.backends['packtivity'].submit(
                    task.spec, task.parameters, task.state, task.metadata
                )
        elif tasktype == init_task:
            #init steps are by definition successful
            return InitProxy(status = 'SUCCESS', result = task.parameters)

    def routeproxy(self, proxy):
        if type(proxy) == InitProxy:
            return 'init', proxy
        else:
            return 'packtivity', proxy
        raise NotImplementedError('needs implementation')

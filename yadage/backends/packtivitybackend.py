import federatedbackend
import logging
import caching
import yadage.yadagestep
from trivialbackend import TrivialProxy, TrivialBackend
from packtivity.backendutils import backend_from_string

log = logging.getLogger(__name__)


class InitProxy(TrivialProxy):
    def proxyname(self):
        return 'InitProxy'


class PacktivityBackend(federatedbackend.FederatedBackend):
    '''
    a backend that mainly submits step tasks to packtivity backend
    except for init nodes, which are resolvable trivially.
    '''

    def __init__(self,  packtivity_backendstring = None, packtivity_backend = None, cacheconfig = None):
        if packtivity_backendstring:
            is_sync, backend = backend_from_string(packtivity_backendstring)
            assert not is_sync
        elif packtivity_backend:
            backend = packtivity_backend
        else:
            raise RuntimeError('need backend or backendstring')
        if cacheconfig:
            self.cached = True
            backend = caching.CachedBackend(
                backend,
                cacheconfig=cacheconfig
            )
        else:
            self.cached = False
        super(PacktivityBackend, self).__init__({
            'init': TrivialBackend(),
            'packtivity': backend
        })

    def routedsubmit(self, task):
        tasktype = type(task)
        if tasktype == yadage.yadagestep.yadagestep:
            #this is a little hacky, because the packtivity backends
            #take unrolled spec/parameters/context while the adage API
            #takes generalized task objects
            #possibly could use Munch on the packtivity side to
            #dynammicaly create .task/.attributes/.context-able objects
            if self.cached:
                #Cached backends adhere to the task-based API
                return self.backends['packtivity'].submit(task)
            else:
                #primary packtivity backends adhere to the unrolled API
                return self.backends['packtivity'].submit(
                    task.spec, task.attributes, task.context
                )
        elif tasktype == yadage.yadagestep.initstep:
            #init steps are by definition successful
            return InitProxy(status = 'SUCCESS', result = task.attributes)

    def routeproxy(self, proxy):
        if type(proxy) == InitProxy:
            return 'init'
        else:
            return 'packtivity'
        raise NotImplementedError('needs implementation')

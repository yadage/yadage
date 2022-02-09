import logging

import yadage.backends.caching as caching
import yadage.backends.federatedbackend as federatedbackend
from packtivity.asyncbackends import ForegroundBackend
from packtivity.backendutils import backend_from_string

log = logging.getLogger(__name__)


class PacktivityBackend(federatedbackend.FederatedBackend):
    """
    a backend that mainly submits step tasks to a packtivity backend
    except for init nodes, which are resolvable trivially.
    """

    def __init__(
        self, packtivity_backendstring=None, packtivity_backend=None, backendopts=None
    ):
        purepubopts = backendopts.pop("purepubopts", {})
        if packtivity_backendstring:
            is_sync, backend = backend_from_string(
                packtivity_backendstring, backendopts=backendopts
            )
            assert not is_sync
        elif packtivity_backend:
            backend = packtivity_backend
        else:
            raise RuntimeError("need backend or backendstring")
        self.cached = False
        super(PacktivityBackend, self).__init__(
            {"purepub": ForegroundBackend(purepubopts), "packtivity": backend}
        )

    def enable_cache(self, cachestring=None, cache=None):
        if not (cache or cachestring):
            raise RuntimeError(
                "need to provide either cache object or cache config string"
            )

        self.cached = True
        self.backends["packtivity"] = caching.CachedBackend(
            self.backends["packtivity"],
            cache=cache if cache else caching.setupcache_fromstring(cachestring),
        )

    def routedbatchsubmit(self, tasks):
        actual_submits = [
            not t.metadata["wflow_hints"].get("is_purepub", False) for t in tasks
        ]
        if all(actual_submits) and not self.cached:
            try:
                submit_data = [
                    (task.spec, task.parameters, task.state, task.metadata)
                    for task in tasks
                ]
                return self.backends["packtivity"].batch_submit(*zip(*submit_data))
                log.warning(actual_submits, self.backends["packtivity"].batched_submit)
            except AttributeError:
                pass
        raise NotImplementedError

    def routedsubmit(self, task):
        is_pure_publishing = task.metadata["wflow_hints"].get("is_purepub", False)
        if is_pure_publishing:
            foreground_proxy = self.backends["purepub"].submit(
                task.spec, task.parameters, task.state, task.metadata
            )
            foreground_proxy.set_details({"labels": {"backend_hints": "is_purepub"}})
            return foreground_proxy

        else:
            # this is a little hacky, because the packtivity backends
            # take unrolled spec/parameters/context while the adage API
            # takes generalized task objects
            # possibly could use Munch on the packtivity side to
            # dynammicaly create .task/.parameters/.state-able objects
            if self.cached:
                # Cached backends adhere to the task-based API
                return self.backends["packtivity"].submit(task)
            else:
                # primary packtivity backends adhere to the unrolled API
                return self.backends["packtivity"].submit(
                    task.spec, task.parameters, task.state, task.metadata
                )

    def expected_result(self, task):
        if self.cached:
            # Cached backends adhere to the task-based API
            return self.backends["packtivity"].expected_result(task)
        else:
            # primary packtivity backends adhere to the unrolled API
            return self.backends["packtivity"].prepublish(
                task.spec, task.parameters, task.state
            )

    def routeproxy(self, proxy):
        details = proxy.details() or {}
        if details.get("labels", {}).get("backend_hints") == "is_purepub":
            return "purepub", proxy
        else:
            return "packtivity", proxy
        raise NotImplementedError("needs implementation")

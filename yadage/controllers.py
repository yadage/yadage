import contextlib
import json
from adage.wflowcontroller import BaseController
import adage.controllerutils as ctrlutils
import logging

from yadage.yadagemodels import YadageWorkflow
from yadage.manualutils import VariableProxy
from adage.wflowcontroller import InMemoryController
import functools

log = logging.getLogger(__name__)

def load_state_custom_deserializer(jsondata, backendstring = None):
    from clihelpers import setupbackend_fromstring
    if backendstring:
        backend = setupbackend_fromstring(backendstring)

    workflow = YadageWorkflow.fromJSON(
        jsondata,
        VariableProxy,
        backend
    )
    return workflow

def setup_controller_fromstring(workflowobj, ctrlstring = 'inmem'):
    if ctrlstring == 'inmem':
        return InMemoryController(workflowobj, backend = None)
    elif ctrlstring.startswith('filebacked'):
        filename = ctrlstring.split(':')[-1]
        model   = FileBackedModel(
            filename = filename,
            deserializer = functools.partial(load_state_custom_deserializer, backendstring = 'celery'),
            initdata = workflowobj
        )
        return StatefulController(model)
    else:
        raise RuntimeError('unknown workflow controller %s', ctrlstring)

class FileBackedModel(object):
    def __init__(self, filename, deserializer, initdata = None):
        self.filename = filename
        self.deserializer = deserializer
        if initdata:
            self.commit(initdata)

    def commit(self, data):
        '''
        :param data: data to commit to disk. needs to have '.json()' method
        commits data (possibly to persistent storage)
        '''
        with open(self.filename,'w') as statefile:
            json.dump(data.json(), statefile)

    def load(self):
        '''
        :return: the adage workflow object holding rules and the graph
        '''
        with open(self.filename) as statefile:
            self.data = self.deserializer(json.load(statefile))
            return self.data

@contextlib.contextmanager
def transaction(model):
    '''
    param: model: a model object with .load() and .commit(data) methods

    '''
    # log.info('loading model')
    data = model.load()
    yield data
    # log.info('committing model')
    model.commit(data)

class StatefulController(BaseController):
    '''
    workflow controller, that explicltly calls transaction methods on non read-only operations on the workflow state
    '''
    def __init__(self, model, backend = None):
        super(StatefulController, self).__init__(backend)
        self.model = model
        self._adageobj = model.load()

    @property
    def adageobj(self):
        return self._adageobj

    def submit_nodes(self, nodeids):
        with transaction(self.model) as self._adageobj:
            nodes = [self._adageobj.dag.getNode(nodeid) for nodeid in nodeids]
            super(StatefulController,self).submit_nodes(nodes)

    def apply_rules(self, ruleids):
        with transaction(self.model) as self._adageobj:
            rules = [r for r in self._adageobj.rules if r.identifier in ruleids]
            super(StatefulController,self).apply_rules(rules)

    def sync_backend(self):
        with transaction(self.model) as self._adageobj:
            super(StatefulController,self).sync_backend()

    def applicable_rules(self):
        '''
        :return: return a list of rules whose predicate is fulfilled
        '''
        a = [x.identifier for x in ctrlutils.applicable_rules(self.adageobj)]
        return a

    def submittable_nodes(self):
        '''
        :return: a list of nodes with sucessfull and completed upstream
        '''
        return [x.identifier for x in ctrlutils.submittable_nodes(self.adageobj)]
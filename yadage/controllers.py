import json
import contextlib
import logging
import functools

import yadage.reset
import adage.controllerutils as ctrlutils
from adage.wflowcontroller import BaseController, InMemoryController
from yadage.wflow import YadageWorkflow


log = logging.getLogger(__name__)

class VariableProxy():
    @staticmethod
    def fromJSON(data):
        import packtivity.asyncbackends
        import yadage.backends.packtivitybackend
        if data['proxyname']=='InitProxy':
            return yadage.backends.packtivitybackend.InitProxy.fromJSON(data)
        elif data['proxyname']=='CeleryProxy':
            return packtivity.asyncbackends.CeleryProxy.fromJSON(data)
        else:
            raise RuntimeError('only celery support for now... found proxy with name: {}'.format(data['proxyname']))

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

def create_model_fromstring(modelidstring):
    modeltype, modelid = modelidstring.split(':')
    if modeltype == 'filebacked':
        return FileBackedModel(
            filename = modelid,
            deserializer = functools.partial(load_state_custom_deserializer, backendstring = 'celery'),
        )
    if modeltype == 'mongo':
        return MongoBackedModel(
            deserializer = functools.partial(load_state_custom_deserializer, backendstring = 'celery'),
            wflowid = modelid
        )

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
        return PersistentController(model)
    elif ctrlstring == 'mongo':
        model = MongoBackedModel(
            deserializer = functools.partial(load_state_custom_deserializer, backendstring = 'celery'),
            initdata = workflowobj
        )
        return PersistentController(model)
    else:
        raise RuntimeError('unknown workflow controller %s', ctrlstring)

class MongoBackedModel(object):
    def __init__(self, deserializer, connect_string = 'mongodb://localhost:27017/', initdata = None, wflowid = None):
        from pymongo import MongoClient
        from bson.objectid import ObjectId
        self.deserializer = deserializer
        self.client = MongoClient(connect_string)
        self.db = self.client.wflowdb
        self.collection = self.db.workflows
        if initdata:
            insertion = self.collection.insert_one(initdata.json())
            self.wflowid = insertion.inserted_id
            log.info('created new workflow object with id %s', str(self.wflowid))
        if wflowid:
            self.wflowid = ObjectId(wflowid)

    def commit(self, data):
        self.collection.replace_one({'_id':self.wflowid}, data.json())

    def load(self):
        return self.deserializer(self.collection.find_one({'_id':self.wflowid}))

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
            return self.deserializer(json.load(statefile))

@contextlib.contextmanager
def model_transaction(self):
    '''
    param: model: a model object with .load() and .commit(data) methods
    '''
    self._adageobj = self.model.load()
    yield

    isvalid = self.validate()
    if not isvalid:
        #raise RuntimeError('was about to commit invalid data!')
        log.warning('commit is in valid %s', isvalid)
    self.model.commit(self._adageobj)

class PersistentController(BaseController):
    '''
    workflow controller, that explicltly calls transaction methods on non read-only operations on the workflow state
    '''
    def __init__(self, model, backend = None):
        super(PersistentController, self).__init__(backend)
        self.model = model
        self._adageobj = self.model.load()


    def transaction(self):
        return model_transaction(self)

    @property
    def adageobj(self):
        return self._adageobj

    def submit_nodes(self, nodeids):
        # log.info('about to submit')
        with self.transaction():
            nodes = [self._adageobj.dag.getNode(nodeid) for nodeid in nodeids]
            # log.info('submitting nodes to backend: %s', nodes)
            super(PersistentController,self).submit_nodes(nodes)

    def apply_rules(self, ruleids):
        with self.transaction():
            rules = [r for r in self._adageobj.rules if r.identifier in ruleids]
            super(PersistentController,self).apply_rules(rules)

    def sync_backend(self):
        with self.transaction():
            super(PersistentController,self).sync_backend()

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

    def reset_nodes(self, nodeids):
        '''
        :param nodes: 
        '''
        with self.transaction():
            yadage.reset.reset_steps(self._adageobj,nodeids)

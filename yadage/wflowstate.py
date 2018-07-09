import json
import logging

from packtivity.statecontexts import load_state

from .backends import load_proxy
from .state_providers import load_provider
from .wflow import YadageWorkflow
from .wflownode import YadageNode

log = logging.getLogger(__name__)


def make_deserializer(deserialization_opts = None):
    def deserializer(jsondata):
        workflow = YadageWorkflow.fromJSON(
            jsondata,
            lambda data: load_proxy(data,deserialization_opts),
            lambda data: load_provider(data,deserialization_opts),
            lambda data: YadageNode.fromJSON(data,
                                state_deserializer =
                                    lambda state: load_state(
                                        state,deserialization_opts
                                    )
            )
        )
        return workflow
    return deserializer


from .handlers.utils import handler_decorator

modelhandlers, statemodel = handler_decorator()

@statemodel('inmem')
def inmem_model(modelsetup, modelopts, initmodel):
    return initmodel

@statemodel('filebacked')
def filebacked_model(modelsetup, modelopts, initmodel):
    filename = modelsetup.split(':')[-1]
    model   = FileBackedModel(
        filename = filename,
        initmodel = initmodel,
        deserializer = make_deserializer(modelopts)
    )
    return model

@statemodel('mongo')
def mongo_model(modelsetup, modelopts, initmodel):
    model = MongoBackedModel(
        initmodel = initmodel,
        deserializer = make_deserializer(modelopts)
    )
    return model

def load_model_fromstring(modelsetup,modelopts = None,initmodel = None):
    modelopts = modelopts or {}
    for k in modelhandlers.keys():
        if modelsetup.startswith(k):
            return modelhandlers[k](modelsetup,modelopts,initmodel)
    raise RuntimeError('unknown state model %s', modelsetup)

class MongoBackedModel(object):
    '''
    model that holds the workflow state in a MongoDB database.
    '''
    def __init__(self, deserializer = None, connect_string = 'mongodb://localhost:27017/', initmodel = None, wflowid = None):
        from pymongo import MongoClient
        from bson.objectid import ObjectId
        self.deserializer = deserializer or make_deserializer()
        self.client = MongoClient(connect_string)
        self.db = self.client.wflowdb
        self.collection = self.db.workflows
        if initmodel:
            insertion = self.collection.insert_one(initmodel.json())
            self.wflowid = insertion.inserted_id
            log.info('created new workflow object with id %s', str(self.wflowid))
        if wflowid:
            self.wflowid = ObjectId(wflowid)

    def commit(self, data):
        '''
        :param data: data to commit to disk. needs to have '.json()' method
        return: None
        '''
        self.collection.replace_one({'_id':self.wflowid}, data.json())

    def load(self):
        '''
        param load: load data from database
        return: yadage workflow object
        '''
        return self.deserializer(self.collection.find_one({'_id':self.wflowid}))

class FileBackedModel(object):
    '''
    model that holds data on disk in a JSON file
    '''
    def __init__(self, filename, deserializer = None, initmodel = None):
        self.filename = filename
        self.deserializer = deserializer or make_deserializer()
        if initmodel:
            self.commit(initmodel)

    def commit(self, data):
        '''
        :param data: data to commit to disk. needs to have '.json()' method
        '''
        log.debug('committing model')
        jsondata = data.json()

        with open(self.filename,'w') as statefile:
            json.dump(jsondata, statefile)

    def load(self):
        '''
        :return: the adage workflow object holding rules and the graph
        '''
        log.debug('loading model')
        with open(self.filename) as statefile:
            jsondata = json.load(statefile)
            return self.deserializer(jsondata)

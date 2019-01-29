import json
import logging

from .wflow import YadageWorkflow
from .handlers.utils import handler_decorator

log = logging.getLogger(__name__)

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
        deserialization_opts = modelopts
    )
    return model

@statemodel('mongo')
def mongo_model(modelsetup, modelopts, initmodel):
    model = MongoBackedModel(
        initmodel = initmodel, deserialization_opts = modelopts
    )
    return model

def load_model_fromstring(modelsetup,modelopts = None,initmodel = None):
    modelopts = modelopts or {}
    for k in modelhandlers.keys():
        if modelsetup.startswith(k):
            return modelhandlers[k](modelsetup,modelopts,initmodel)
    raise RuntimeError('unknown state model %s' % modelsetup)

class MongoBackedModel(object):
    '''
    model that holds the workflow state in a MongoDB database.
    '''
    def __init__(self, deserialization_opts = None, connect_string = 'mongodb://localhost:27017/', initmodel = None, wflowid = None):
        from pymongo import MongoClient
        from bson.objectid import ObjectId
        self.deserialization_opts = deserialization_opts
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
        jsondata = self.collection.find_one({'_id':self.wflowid})
        return YadageWorkflow.fromJSON(jsondata,self.deserialization_opts)

class FileBackedModel(object):
    '''
    model that holds data on disk in a JSON file
    '''
    def __init__(self, filename, deserialization_opts = None, initmodel = None):
        self.filename = filename
        self.deserialization_opts = deserialization_opts
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
            return YadageWorkflow.fromJSON(jsondata,self.deserialization_opts)

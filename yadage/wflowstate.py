import json
import logging
from .wflow import YadageWorkflow
from .backends import load_proxy

log = logging.getLogger(__name__)

def load_model(jsondata):
    workflow = YadageWorkflow.fromJSON(
        jsondata,
        load_proxy
    )
    return workflow

def load_model_fromstring(modelidstring):
    modeltype, modelid = modelidstring.split(':')
    if modeltype == 'filebacked':
        return FileBackedModel(
            filename = modelid,
            deserializer = load_model
        )
    if modeltype == 'mongo':
        return MongoBackedModel(
            deserializer = load_model,
            wflowid = modelid
        )
    raise RuntimeError('unknown model string')

class MongoBackedModel(object):
    '''
    model that holds the workflow state in a MongoDB database.
    '''
    def __init__(self, deserializer = load_model, connect_string = 'mongodb://localhost:27017/', initdata = None, wflowid = None):
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
    def __init__(self, filename, deserializer = load_model, initdata = None):
        self.filename = filename
        self.deserializer = deserializer
        if initdata:
            self.commit(initdata)

    def commit(self, data):
        '''
        :param data: data to commit to disk. needs to have '.json()' method
        '''
        with open(self.filename,'w') as statefile:
            json.dump(data.json(), statefile)

    def load(self):
        '''
        :return: the adage workflow object holding rules and the graph
        '''
        with open(self.filename) as statefile:
            return self.deserializer(json.load(statefile))


import json
import contextlib
import logging
from yadage.wflow import YadageWorkflow

log = logging.getLogger(__name__)


def load_proxy(data):
        import packtivity.asyncbackends
        import yadage.backends.packtivitybackend
        if data['proxyname']=='InitProxy':
            return yadage.backends.packtivitybackend.InitProxy.fromJSON(data)
        elif data['proxyname']=='CeleryProxy':
            return packtivity.asyncbackends.CeleryProxy.fromJSON(data)
        else:
            raise RuntimeError('only celery support for now... found proxy with name: {}'.format(data['proxyname']))

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
    self.adageobj = self.model.load()
    yield

    isvalid = self.validate()
    if not isvalid:
        #raise RuntimeError('was about to commit invalid data!')
        log.warning('commit is in valid %s', isvalid)
    self.model.commit(self.adageobj)

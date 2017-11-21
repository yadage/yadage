import adage
from adage.serialize import obj_to_json
from .wflowview import WorkflowView
from .stages import JsonStage, OffsetStage

def json_or_nil(x):
    return None if x is None else x.json()

def what(x):
    # raise RuntimeError(x.json()['metadata']['wflow_hints'])
    return None if x is None else x.json()


class YadageWorkflow(adage.adageobject):
    '''
    The overall workflow state object that extends the basic
    Adage state object by two bookkeeping structures.
    '''

    def __init__(self):
        super(YadageWorkflow, self).__init__()
        self.stepsbystage = {}
        self.bookkeeping = {}

    def view(self, offset=''):
        return WorkflowView(self, offset)

    def json(self):
        data = obj_to_json(self,
                           ruleserializer=json_or_nil,
                           taskserializer=what,
                           proxyserializer=json_or_nil,
                           )
        data['bookkeeping'] = self.bookkeeping
        data['stepsbystage'] = self.stepsbystage
        return data

    @classmethod
    def fromJSON(cls, data,
            proxydeserializer = lambda data: None,
            state_provider_deserializer = lambda data: None,
            task_deserializer = lambda data: None,
            backend=None):
        instance = cls()
        instance.rules = [OffsetStage.fromJSON(x,state_provider_deserializer) for x in data['rules'] ]
        instance.applied_rules = [OffsetStage.fromJSON(x,state_provider_deserializer) for x in data['applied'] ]
        instance.bookkeeping = data['bookkeeping']
        instance.stepsbystage = data['stepsbystage']

        instance.dag = adage.serialize.dag_from_json(
            data['dag'],
            task_deserializer,
            proxydeserializer,
            backend
        )
        return instance

    @classmethod
    def createFromJSON(cls, jsondata, state_provider):
        instance = cls()
        rules = [JsonStage(stagedata, state_provider) for stagedata in jsondata['stages']]
        instance.view().addWorkflow(rules)
        return instance

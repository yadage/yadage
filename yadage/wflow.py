import adage
from backends import NoneProxy
from adage.serialize import obj_to_json
from wflowview import WorkflowView, offsetRule
from wflownode import YadageNode
from stages import jsonStage

def json_or_nil(x):
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
                           taskserializer=json_or_nil,
                           proxyserializer=json_or_nil,
                           )
        data['bookkeeping'] = self.bookkeeping
        data['stepsbystage'] = self.stepsbystage
        return data

    @classmethod
    def fromJSON(cls, data, proxyclass=NoneProxy, backend=None):
        instance = cls()
        instance.rules = [offsetRule.fromJSON(x) for x in data['rules'] ]
        instance.applied_rules = [offsetRule.fromJSON(x) for x in data['applied'] ]
        instance.bookkeeping = data['bookkeeping']
        instance.stepsbystage = data['stepsbystage']

        instance.dag = adage.serialize.dag_from_json(
            data['dag'],
            YadageNode,
            proxyclass,
            backend
        )
        return instance

    @classmethod
    def createFromJSON(cls, jsondata, state_provider):
        instance = cls()
        rules = [jsonStage(stagedata, state_provider) for stagedata in jsondata['stages']]
        instance.view().addWorkflow(rules)
        return instance
import logging
import yadage.reset
import adage.controllerutils as ctrlutils
from adage.wflowcontroller import BaseController, InMemoryController
from wflowstate import FileBackedModel, MongoBackedModel, model_transaction, load_model
log = logging.getLogger(__name__)

def setup_controller_from_statestring(workflowobj, statestr = 'inmem'):
    '''
    return controller instance based on state configuration. For
    transaction-based states, returns PersistentController, for in-
    memory states returns in InMemoryController
    '''
    if statestr == 'inmem':
        return InMemoryController(workflowobj)
    elif statestr.startswith('filebacked'):
        filename = statestr.split(':')[-1]
        model   = FileBackedModel(
            filename = filename,
            initdata = workflowobj
        )
        return PersistentController(model)
    elif statestr == 'mongo':
        model = MongoBackedModel(
            initdata = workflowobj
        )
        return PersistentController(model)
    else:
        raise RuntimeError('unknown workflow state spec %s', statestr)

class PersistentController(BaseController):
    '''
    workflow controller, that explicltly calls transaction methods on non read-only operations on the workflow state
    '''
    def __init__(self, model, backend = None):
        self.model = model
        super(PersistentController, self).__init__(self.model.load(),backend)

    def transaction(self):
        return model_transaction(self)

    def submit_nodes(self, nodeids):
        '''
        submit nodes to backend

        :param nodeids: list of ids of nodes to be submitted
        :return: None
        '''
        with self.transaction():
            nodes = [self.adageobj.dag.getNode(nodeid) for nodeid in nodeids]
            # log.info('submitting nodes to backend: %s', nodes)
            super(PersistentController,self).submit_nodes(nodes)

    def apply_rules(self, ruleids):
        '''
        apply rules to workflow instance

        :param ruleids: list of ids of reuls to be applied
        :return: None
        '''
        with self.transaction():
            rules = [r for r in self.adageobj.rules if r.identifier in ruleids]
            super(PersistentController,self).apply_rules(rules)

    def sync_backend(self):
        '''
        synchronize node data with backend
        '''
        with self.transaction():
            super(PersistentController,self).sync_backend()

    def applicable_rules(self):
        '''
        :return: a list of rules whose predicate is fulfilled
        '''
        applicable_rules = super(PersistentController,self).applicable_rules()
        return [x.identifier for x in applicable_rules]

    def submittable_nodes(self):
        '''
        :return: a list of nodes with sucessfull and completed upstream
        '''
        submittable_nodes = super(PersistentController,self).submittable_nodes()
        return [x.identifier for x in submittable_nodes]

    def reset_nodes(self, nodeids):
        '''
        :param nodeids: list of ids of nodes to reset
        :return: None
        '''
        with self.transaction():
            yadage.reset.reset_steps(self.adageobj,nodeids)

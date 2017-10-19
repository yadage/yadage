import logging
import contextlib

from adage.wflowcontroller import BaseController
from yadage.wflowstate import load_model_fromstring
from yadage.reset import reset_steps

log = logging.getLogger(__name__)

def setup_controller_from_modelstring(workflowobj = None, modelsetup = 'inmem', modelopts = None):
    '''
    return controller instance based on state configuration. For
    transaction-based states, returns PersistentController, for in-
    memory states returns in BaseController
    '''
    modelopts = modelopts or {}
    if modelsetup == 'inmem':
        return BaseController(workflowobj)
    else:
        model = load_model_fromstring(modelsetup,modelopts,workflowobj)
        return PersistentController(model)

class PersistentController(BaseController):
    '''
    workflow controller, that explicltly calls transaction methods on non read-only operations on the workflow state
    '''
    def __init__(self, model, backend = None):
        '''
        :param model: the model on whih the controller will operate
        :param backend: the backend to against which to check workflow state.

        :return: the controller instance
        '''
        self.model = model
        super(PersistentController, self).__init__(self.model.load(),backend)


    @contextlib.contextmanager
    def transaction(self):
        '''the transaction context. will commit model to persistent store on exit.'''
        self.adageobj = self.model.load()
        yield

        isvalid = self.validate()
        if not isvalid:
            #raise RuntimeError('was about to commit invalid data!')
            log.warning('commit is in valid %s', isvalid)
        self.model.commit(self.adageobj)

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
            reset_steps(self.adageobj,nodeids)

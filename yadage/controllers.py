import contextlib
import importlib
import logging
import os
from adage.wflowcontroller import BaseController
from packtivity.syncbackends import defaultsyncbackend

from .reset import collective_downstream, remove_rules, reset_steps, undo_rules
from .wflow import YadageWorkflow
from .handlers.utils import handler_decorator

log = logging.getLogger(__name__)

ctrlhandlers, controller = handler_decorator()

class YadageController(BaseController):
    def __init__(self,*args, **kwargs):
        self.prepublishing_backend = defaultsyncbackend()
        self.disable_backend = False
        self.disable_prepublishing = kwargs.pop('disable_prepub',False)
        super(YadageController,self).__init__(*args,**kwargs)

    def sync_expected(self):
        for n in self.adageobj.dag.nodes():
            if 'YADAGE_IGNORE_PREPUBLISHING' in os.environ or self.disable_prepublishing:
                continue
            node = self.adageobj.dag.getNode(n)
            node.expected_result = self.prepublishing_backend.prepublish(
                node.task.spec, node.task.parameters.json(), node.task.state
            )

    def sync_backend(self):
        self.sync_expected()
        if not self.disable_backend:
            super(YadageController,self).sync_backend()

@controller('frommodel')
def frommodel_controller(ctrlstring, ctrlopts, model = None):
    if isinstance(model, YadageWorkflow):
        return YadageController(model,**ctrlopts)
    else:
        return PersistentController(model,**ctrlopts)

@controller('http')
def http_controller(ctrlstring, ctrlopts, model = None):
    try:
        from yadagehttpctrl.clientcontroller import YadageHTTPController
        ctrl = YadageHTTPController(server = ctrlstring, **ctrlopts)
        return ctrl
    except ImportError:
        log.exception('try installing yadagehttpctrl')

@controller('py:')
def frompython_controller(ctrlstring, ctrlopts, model = None):
    _, module, ctrlclass = ctrlstring.split(':')
    module = importlib.import_module(module)
    ctrlclass = getattr(module,ctrlclass)
    if ctrlopts.pop('pass_model',False):
        ctrlopts['model'] = model
    return ctrlclass(**ctrlopts)

def setup_controller(model = None, controller = 'frommodel', ctrlopts = None):
    '''
    return controller instance based on state configuration. For
    transaction-based states, returns PersistentController, for in-
    memory states returns in BaseController
    '''
    ctrlopts  = ctrlopts or {}
    for k in ctrlhandlers.keys():
        if controller.startswith(k):
            return ctrlhandlers[k](controller, ctrlopts, model)
    raise RuntimeError('unknown controller type %s' % controller)

class PersistentController(YadageController):
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
    def transaction(self, sync = True):
        '''the transaction context. will commit model to persistent store on exit.'''
        self.adageobj = self.model.load()
        if sync:
            log.debug('syncing to setup tx %s', self)
            super(PersistentController,self).sync_backend()
        yield

        isvalid = self.validate()
        if not isvalid:
            #raise RuntimeError('was about to commit invalid data!')
            log.warning('commit is invalid %s', isvalid)
        if sync:
            log.debug('syncing to teardown tx %s', self)
            super(PersistentController,self).sync_backend()
        self.model.commit(self.adageobj)

    def submit_nodes(self, nodeids):
        '''
        submit nodes to backend

        :param nodeids: list of ids of nodes to be submitted
        :return: None
        '''
        log.debug('transaction to submit')
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
        log.debug('transaction to apply')
        with self.transaction():
            rules = [r for r in self.adageobj.rules if r.identifier in ruleids]
            super(PersistentController,self).apply_rules(rules)

    def sync_backend(self):
        '''
        synchronize node data with backend
        '''
        log.debug('transaction to sync but (without sync in tx)')
        with self.transaction(sync = False): # disable sync to avoid infinite recursion
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

    def add_rules(self, rulespecs, dataarg, offset = '', groupname = None , dataopts = None):
        log.debug('adding %s rules', len(rulespecs))
        from .stages import JsonStage
        from .state_providers  import state_provider_from_string
        sp = state_provider_from_string(dataarg,dataopts)
        rules = [JsonStage(json, sp) for json in rulespecs]
        with self.transaction():
            self.adageobj.view(offset).addWorkflow(rules,groupname)

    def patch_rule(self, ruleid, patchspec):
        with self.transaction():
            rule = [x for x in self.adageobj.rules if x.identifier == ruleid][0]
            rule.rule.stagespec = patchspec

    def undo_rules(self, ruleids):
        '''
        :param nodeids: list of ids of nodes to reset
        :return: None
        '''
        with self.transaction():
            undo_rules(self.adageobj, ruleids)

    def remove_rules(self, ruleids):
        with self.transaction():
            remove_rules(self.adageobj, ruleids)

    def reset_nodes(self, nodeids):
        '''
        :param nodeids: list of ids of nodes to reset
        :return: None
        '''
        to_reset = nodeids + collective_downstream(self.adageobj, nodeids)
        with self.transaction():
            reset_steps(self.adageobj,to_reset)

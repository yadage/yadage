import adage
import adage.backends
import os
import json
import workflow_loader
import utils
import visualize
import serialize
import yadageschemas
import shutil
import logging
from packtivity.statecontexts.posixfs_context import LocalFSProvider, LocalFSState

from controllers import setup_controller_from_statestring
from wflow import YadageWorkflow

log = logging.getLogger(__name__)

class YadageSteering():
    '''
    high level steering object to manage worklfow execution
    '''

    def __init__(self,loggername = __name__):
        self.log = logging.getLogger(loggername)
        self.metadir = None
        self.controller = None
        self.rootprovider = None
        self.adage_kwargs = {}

    @property
    def workflow(self):
        return self.controller.adageobj

    def prepare_workdir(self, workdir, accept_existing_workdir = False, stateinit = None, metadir = None):
        writable_state    = LocalFSState([workdir])
        self.rootprovider = LocalFSProvider(stateinit,writable_state, ensure = True, nest = True)
        self.metadir = metadir or '{}/_yadage/'.format(workdir)

        if os.path.exists(self.metadir):
            if not accept_existing_workdir:
                raise RuntimeError('yadage meta directory exists. explicitly accept')
        else:
            os.makedirs(self.metadir)
    
    def init_workflow(self, workflow, toplevel, initdata = None, statesetup = 'inmem', initdir = None, search_initdir = True, validate = True, schemadir = yadageschemas.schemadir):
        ##check input data
        if not initdata:
            initdata = {}
        if search_initdir and initdir:
            utils.discover_initfiles(initdata,os.path.realpath(initdir))

        workflow_json = workflow_loader.workflow(
            workflow,
            toplevel=toplevel,
            schemadir=schemadir,
            validate=validate
        )
        with open('{}/yadage_template.json'.format(self.metadir), 'w') as f:
            json.dump(workflow_json, f)
        workflowobj = YadageWorkflow.createFromJSON(workflow_json, self.rootprovider)
        if initdata:
            log.info('initializing workflow with %s',initdata)
            workflowobj.view().init(initdata)
        else:
            log.info('no initialization data')

        self.controller = setup_controller_from_statestring(workflowobj, statestr = statesetup)

    def adage_argument(self,**kwargs):
        self.adage_kwargs.update(**kwargs)

    def run_adage(self, backend, **adage_kwargs):
        self.controller.backend = backend
        self.adage_argument(**adage_kwargs)
        adage.rundag(controller = self.controller, **self.adage_kwargs)

    def serialize(self):
        serialize.snapshot(
            self.workflow,
            '{}/yadage_snapshot_workflow.json'.format(self.metadir),
            '{}/yadage_snapshot_backend.json'.format(self.metadir)
        )

    def visualize(self):
        visualize.write_prov_graph(self.metadir, self.workflow, vizformat='png')
        visualize.write_prov_graph(self.metadir, self.workflow, vizformat='pdf')


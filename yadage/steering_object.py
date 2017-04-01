import adage
import adage.backends
import os
import json
import workflow_loader
from yadage.controllers import setup_controller_fromstring
from yadage.yadagemodels import YadageWorkflow
import clihelpers
import visualize
import serialize
import yadageschemas
import shutil
import logging
import packtivity.statecontexts.posixfs_context as statecontext

log = logging.getLogger(__name__)
class YadageSteering():
    def __init__(self,loggername = __name__):
        self.log = logging.getLogger(loggername)
        self.workdir = None
        self.yadagedir = None
        self.controller = None
        self.rootcontext = None
        self.adage_kwargs = {}

    @property
    def workflow(self):
        return self.controller.adageobj

    def prepare_workdir(self, workdir, accept_existing_workdir = False, contextinit = None):
        self.workdir = workdir
        self.rootcontext = contextinit or {}
        self.rootcontext = statecontext.merge_contexts(self.rootcontext,statecontext.make_new_context(workdir))
        self.yadagedir = '{}/_yadage/'.format(workdir)

        if os.path.exists(self.yadagedir):
            if not accept_existing_workdir:
                raise RuntimeError('yadage meta directory exists. explicitly accept')
            self.log.info('yadage meta directory exists.. will remove and remake')
            shutil.rmtree(self.yadagedir)
        os.makedirs(self.yadagedir)
    
    def init_workflow(self, workflow, toplevel, initdata, ctrlsetup = 'inmem', initdir = None, search_initdir = True, validate = True, schemadir = yadageschemas.schemadir):
        ##check input data
        if search_initdir and initdir:
            clihelpers.discover_initfiles(initdata,os.path.realpath(initdir))

        workflow_json = workflow_loader.workflow(
            workflow,
            toplevel=toplevel,
            schemadir=schemadir,
            validate=validate
        )
        with open('{}/yadage_template.json'.format(self.yadagedir), 'w') as f:
            json.dump(workflow_json, f)
        workflowobj = YadageWorkflow.createFromJSON(workflow_json, self.rootcontext)
        if initdata:
            log.info('initializing workflow with %s',initdata)
            workflowobj.view().init(initdata)
        else:
            log.info('no initialization data')

        self.controller = setup_controller_fromstring(workflowobj, ctrlsetup)

    def adage_argument(self,**kwargs):
        self.adage_kwargs.update(**kwargs)

    def run_adage(self, backend, **adage_kwargs):
        self.controller.backend = backend
        self.adage_argument(**adage_kwargs)
        adage.rundag(controller = self.controller, **self.adage_kwargs)

    def serialize(self):
        serialize.snapshot(
            self.workflow,
            '{}/yadage_snapshot_workflow.json'.format(self.yadagedir),
            '{}/yadage_snapshot_backend.json'.format(self.yadagedir)
        )

    def visualize(self):
        visualize.write_prov_graph(self.yadagedir, self.workflow, vizformat='png')
        visualize.write_prov_graph(self.yadagedir, self.workflow, vizformat='pdf')


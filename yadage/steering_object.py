import adage
import adage.backends
import os
import json
import workflow_loader
from yadage.yadagemodels import YadageWorkflow
import visualize
import serialize
import yadageschemas
import shutil
import logging
import packtivity.statecontexts.poxisfs_context as statecontext

class YadageSteering():
    def __init__(self,logger = None):
        self.log = logger or logging.getLogger(__name__)
        self.workdir = None
        self.yadagedir = None
        self.workflow = None
        self.adage_kwargs = {}

    def prepare_workdir(self, workdir, accept_existing_workdir):
        self.workdir = workdir
        self.rootcontext = statecontext.make_new_context(workdir)
        self.yadagedir = '{}/_yadage/'.format(workdir)

        if os.path.exists(self.yadagedir):
            if not accept_existing_workdir:
                raise RuntimeError('yadage meta directory exists. explicitly accept')
            self.log.info('yadage meta directory exists.. will remove and remake')
            shutil.rmtree(self.yadagedir)
        os.makedirs(self.yadagedir)
    
    def init_workflow(self,workflow, toplevel, initdata, search_initdir = True, validate = True, schemadir = yadageschemas.schemadir):
        ##check input data
        if search_initdir:
            for k, v in initdata.iteritems():
                candpath = '{}/init/{}'.format(self.workdir, v)
                if os.path.exists(candpath):
                    initdata[k] = '{}/init/{}'.format(self.rootcontext['readwrite'][0], v)

        workflow_json = workflow_loader.workflow(
            workflow,
            toplevel=toplevel,
            schemadir=schemadir,
            validate=validate
        )
        with open('{}/yadage_template.json'.format(self.yadagedir), 'w') as f:
            json.dump(workflow_json, f)
        self.workflow = YadageWorkflow.createFromJSON(workflow_json, self.rootcontext)
        self.workflow.view().init(initdata)

    def adage_argument(self,**kwargs):
        self.adage_kwargs.update(**kwargs)

    def run_adage(self,**kwargs):
        self.adage_argument(**kwargs)
        adage.rundag(self.workflow,**self.adage_kwargs)

    def serialize(self):
        serialize.snapshot(
            self.workflow,
            '{}/yadage_snapshot_workflow.json'.format(self.yadagedir),
            '{}/yadage_snapshot_backend.json'.format(self.yadagedir)
        )

    def visualize(self):
        visualize.write_prov_graph(self.yadagedir, self.workflow, vizformat='png')
        visualize.write_prov_graph(self.yadagedir, self.workflow, vizformat='pdf')


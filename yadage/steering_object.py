import adage
import adage.backends
import os
import json
import yadageschemas
import logging

import yadage.workflow_loader as workflow_loader
import yadage.utils as utils
import yadage.serialize as serialize
from .controllers import setup_controller_from_modelstring
from .wflow import YadageWorkflow
from .utils import setupbackend_fromstring

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
        self.initdata = {}
        self.adage_kwargs = {}

    @property
    def workflow(self):
        '''
        :return: the workflow object (from the controller)
        '''
        return self.controller.adageobj

    def prepare_meta(self,metadir = None, accept=False):
        '''
        prepare workflow meta-data directory

        :param metadir: the meta-data directory name
        :param accept: whether to accept an existing metadata directory
        '''
        self.metadir = self.metadir or metadir #maybe it's already set
        assert self.metadir
        if os.path.exists(self.metadir):
            if not accept:
                raise RuntimeError('yadage meta directory exists. explicitly accept')
        else:
            os.makedirs(self.metadir)
        self.adage_argument(workdir = os.path.join(self.metadir,'adage'))

    def prepare(self, dataarg, dataopts = None, accept_metadir = False, metadir = None):
        '''
        prepares workflow data state, with  possible initialization and sets up stateprovider used for workflow stages.
        if initialization data is provided, it may be mutated to reflect automatic data discovery

        :param dataarg: mandatory state provider setup. generally <datatype>:<argument>. For
        :param dataopts: optional settings for state provider
        :param accept_metadir:
        :param metadir: meta-data directory
        '''

        self.metadir = metadir
        dataopts = dataopts or {}
        split_dataarg = dataarg.split(':',1)
        if len(split_dataarg) == 1:
            dataarg = split_dataarg[0]
            self.rootprovider = utils.rootprovider_from_string('local:'+dataarg, dataopts)
            self.metadir = self.metadir or '{}/_yadage/'.format(dataarg)
        else:
            assert self.metadir #for non-default provider, require metadir to be set
            self.rootprovider = utils.rootprovider_from_string(dataarg, dataopts)
        self.prepare_meta(accept = accept_metadir)

    def init_workflow(self,
                      workflow = None,
                      initdata = None,
                      toplevel = os.getcwd(),
                      workflow_json = None,
                      modelsetup = 'inmem',
                      modelopts = None,
                      validate = True,
                      schemadir = yadageschemas.schemadir):
        '''
        load workflow from spec and initialize it

        :param workflow: the workflow spec source
        :param toplevel: base URI against which to resolve JSON references in the spec
        :param initdata: initialization data for workflow
        '''

        if not self.rootprovider:
            raise RuntimeError('need to setup root state provider first. run .prepare() first')

        if not workflow_json and not workflow:
            raise RuntimeError('need to provide either direct workflow spec or source to load from')

        if workflow_json:
            if validate: workflow_loader.validate(workflow_json)
        else:
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
            workflowobj.view().init(initdata, self.rootprovider, discover = True)
        else:
            log.info('no initialization data')
        self.controller = setup_controller_from_modelstring(
            workflowobj, modelsetup = modelsetup, modelopts = modelopts
        )

    def adage_argument(self,**kwargs):
        '''
        add keyword arguments for workflow execution (adage)

        :param kwargs: adage keyword arguments (see adage documentation for options)
        '''
        self.adage_kwargs.update(**kwargs)

    def run_adage(self, backend = None, **adage_kwargs):
        '''
        execution workflow with adage based against given backend
        :param backend: backend to use for packtivity processing.
        '''
        self.controller.backend = backend or setupbackend_fromstring('multiproc:auto')
        self.adage_argument(**adage_kwargs)
        adage.rundag(controller = self.controller, **self.adage_kwargs)

    def serialize(self):
        '''
        serialized workflow and backend states (stored in meta directory)
        '''
        serialize.snapshot(
            self.workflow,
            '{}/yadage_snapshot_workflow.json'.format(self.metadir),
            '{}/yadage_snapshot_backend.json'.format(self.metadir)
        )

    def visualize(self):
        '''
        generate workflow visualization (stored in meta directory)
        '''
        import yadage.visualize as visualize
        visualize.write_prov_graph(self.metadir, self.workflow, vizformat='png')
        visualize.write_prov_graph(self.metadir, self.workflow, vizformat='pdf')

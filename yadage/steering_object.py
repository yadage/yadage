import adage
import adage.backends
import os
import json
import yadageschemas
import logging

import yadage.workflow_loader as workflow_loader
import yadage.utils as utils
import yadage.serialize as serialize
from .controllers import setup_controller
from .wflowstate import load_model_fromstring
from .wflow import YadageWorkflow
from .utils import setupbackend_fromstring

log = logging.getLogger(__name__)

class YadageSteering(object):
    '''
    high level steering object to manage worklfow execution
    '''
    def __init__(self,loggername = __name__):
        self.log = logging.getLogger(loggername)
        self.metadir = None
        self.controller = None
        self.rootprovider = None
        self.adage_kwargs = {}

    @classmethod
    def connect(cls, metadir, ctrlstring, ctrlopts = None, modelsetup = None, modelopts = None, accept_metadir = False):
        model = None
        instance = cls()
        instance.prepare_meta(metadir, accept = accept_metadir)
        if modelsetup:
            model = load_model_fromstring(modelsetup,modelopts)
        instance.controller = setup_controller(
            model = model,
            controller = ctrlstring, ctrlopts = ctrlopts,
        )
        log.info('connected to model')
        return instance

    @classmethod
    def create(
        cls,
        dataarg = None,
        dataopts = None,
        workflow_json = None,
        workflow = None,
        toplevel = os.getcwd(),
        schemadir = yadageschemas.schemadir,
        validate=True,
        initdata = None,
        controller = 'frommodel',
        ctrlopts = None,
        metadir = None,
        accept_metadir = False,
        modelsetup = 'inmem',
        modelopts = None):

        ys = cls()
        if workflow_json:
            wflow_kwargs = dict(
                workflow_json = workflow_json
            )
        elif workflow:
            wflow_kwargs = dict(
                workflow = workflow, toplevel = toplevel,
                validate = validate, schemadir = schemadir
            )
        else:
            raise RuntimeError('need to initialize either from full JSON or remote location')

        ys.prepare(
            dataarg = dataarg, dataopts = dataopts,
            metadir = metadir, accept_metadir = accept_metadir,
        )

        ys.init_workflow(
            initdata = initdata,
            modelsetup = modelsetup,
            modelopts = modelopts,
            controller = controller,
            ctrlopts = ctrlopts,
            **wflow_kwargs
        )
        return ys



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
                raise RuntimeError('yadage meta directory %s exists. explicitly accept', self.metadir)
        else:
            os.makedirs(self.metadir)
        self.adage_argument(workdir = os.path.join(self.metadir,'adage'))

    def prepare(self, dataarg, dataopts = None, accept_metadir = False, metadir = None):
        '''
        prepares workflow data state, with  possible initialization and sets up stateprovider used for workflow stages.
        if initialization data is provided

        sets self.metadir and self.rootprovider

        :param dataarg: mandatory state provider setup. generally <datatype>:<argument>. For
        :param dataopts: optional settings for state provider
        :param accept_metadir:
        :param metadir: meta-data directory
        '''

        self.metadir = metadir
        self.rootprovider = utils.state_provider_from_string(dataarg, dataopts)
        dataopts = dataopts or {}
        is_local_data = len(dataarg.split(':',1)) == 1
        if is_local_data:
            self.metadir = self.metadir or '{}/_yadage/'.format(dataarg)
        else:
            assert self.metadir #for non-default provider, require metadir to be set
        self.prepare_meta(accept = accept_metadir)

    def init_workflow(self,
                      workflow = None,
                      initdata = None,
                      toplevel = os.getcwd(),
                      workflow_json = None,
                      modelsetup = 'inmem',
                      modelopts = None,
                      controller = 'frommodel',
                      ctrlopts = None,
                      validate = True,
                      schemadir = yadageschemas.schemadir):
        '''
        load workflow from spec and initialize it

        :param workflow: the workflow spec source
        :param toplevel: base URI against which to resolve JSON references in the spec
        :param initdata: initialization data for workflow

        prepares initial workflow object and sets self.controller
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

        model = load_model_fromstring(modelsetup,modelopts,workflowobj)
        self.controller = setup_controller(
            model = model,
            controller = controller, ctrlopts = ctrlopts,
        )

    def adage_argument(self,**kwargs):
        '''
        add keyword arguments for workflow execution (adage)

        :param kwargs: adage keyword arguments (see adage documentation for options)
        '''
        self.adage_kwargs.update(**kwargs)

    def run_adage(self, backend = 'auto', **adage_kwargs):
        '''
        execution workflow with adage based against given backend
        :param backend: backend to use for packtivity processing.
        '''
        if backend=='auto':
            #respect if the controller already has a backend wired up
            backend = self.controller.backend or setupbackend_fromstring('multiproc:auto')
            log.info('backend automatically set to %s', backend)
        elif backend:
            self.controller.backend = backend
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

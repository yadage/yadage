import logging
import os
import copy

import adage

from .serialize import snapshot
from .wflowstate import load_model_fromstring
from .controllers import setup_controller
from .utils import setupbackend_fromstring, prepare_meta
from .creators import handlers as creators

log = logging.getLogger(__name__)

class YadageSteering(object):
    '''
    high level steering object to manage worklfow execution
    '''
    def __init__(self, metadir, controller):
        self.metadir = metadir
        self.controller = controller
        self.adage_kwargs = dict(workdir = os.path.join(metadir,'adage'))

    @classmethod
    def connect(cls, metadir, ctrlstring, ctrlopts = None, modelsetup = None, modelopts = None, accept_metadir = False):
        prepare_meta(metadir, accept_metadir)

        model = None
        if modelsetup:
            model = load_model_fromstring(modelsetup,modelopts)

        ctrl = setup_controller(
            model = model, controller = ctrlstring, ctrlopts = ctrlopts,
        )
        log.info('connected to model')
        return cls(metadir, ctrl)

    @classmethod
    def create(cls,**kwargs):
        dataarg = kwargs['dataarg']
        is_local_data = len(dataarg.split(':',1)) == 1
        if is_local_data:
            metadir = kwargs.get('metadir')
            metadir = metadir or '{}/_yadage/'.format(dataarg)
        else:
            metadir = kwargs['metadir']
        accept_metadir = kwargs.pop('accept_metadir', False)
        prepare_meta(metadir, accept_metadir)

        kw = copy.deepcopy(kwargs)
        kw['metadir'] = metadir

        ctrl = creators['local'](**kw)
        return cls(metadir, ctrl)

    @property
    def workflow(self):
        '''
        :return: the workflow object (from the controller)
        '''
        return self.controller.adageobj

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
            self.controller.backend = self.controller.backend or setupbackend_fromstring('multiproc:auto')
            log.info('backend automatically set to %s', backend)
        elif backend:
            self.controller.backend = backend

        assert self.controller.backend
        self.adage_argument(**adage_kwargs)
        adage.rundag(controller = self.controller, **self.adage_kwargs)

    def serialize(self):
        '''
        serialized workflow and backend states (stored in meta directory)
        '''
        snapshot(
            self.workflow,
            '{}/yadage_snapshot_workflow.json'.format(self.metadir),
        )

    def visualize(self):
        '''
        generate workflow visualization (stored in meta directory)
        '''
        import yadage.visualize as visualize
        visualize.write_prov_graph(self.metadir, self.workflow, vizformat='png')
        visualize.write_prov_graph(self.metadir, self.workflow, vizformat='pdf')

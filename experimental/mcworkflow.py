from yadage.yadagestep import yadagestep, initstep
import utils
import logging
import yaml
import adage
import adage.backends
import os
from yadage.visualize import write_prov_graph
from newsetup import workflow, stage

logging.basicConfig(level = logging.INFO)


steps = yaml.load(open('steps.yml'))
wflow = workflow()

@stage.fromfunc(wflow, name = 'init')
def init(self):
    init = initstep(self.context['init'])
    self.addStep(init)

@stage.fromfunc(wflow, name = 'prepare', after = 'init')
def prepare(self):
    prep = yadagestep('prepare',steps['prepare'],self.context)

    initstep = self.workflow.element('init').steps[0].identifier

    par1 = utils.read_input(self.dag,prep,(initstep,'couplingA',None))
    par2 = utils.read_input(self.dag,prep,(initstep,'couplingB',None))

    arguments = dict(
        par1 = par1, par2 = par2, param_card = '/workdir/param_card.dat'
    )
    self.addStep(prep.s(**arguments))

@stage.fromfunc(wflow, name = 'grid', after = 'prepare')
def grid(self):

    grid = yadagestep('grid',steps['grid'],self.context)
    prepstep = self.workflow.element('prepare').steps[0].identifier
    
    paramcard = utils.read_input(self.dag,grid,(prepstep,'param_card',None))
    arguments = dict(
        param_card = paramcard,
        gridpack ='/workdir/grid.tar.gz'
    )
    self.addStep(grid.s(**arguments))

@stage.fromfunc(wflow, name = 'madgraph', after = 'grid')
def madgraph(self):
    gridpack_ref = (self.workflow.element('grid').steps[0].identifier,'gridpack',None)
    for index,seed_ref in enumerate(utils.regex_match_outputs([self.workflow.element('init')],['seeds'])):

        mad      = yadagestep('madgraph_{}'.format(index),steps['madgraph'],self.context)
        gridpack = utils.read_input(self.dag,mad,gridpack_ref)
        seed     = utils.read_input(self.dag,mad,seed_ref)

        arguments = dict(
            gridpack = gridpack,
            nevents = 25000,
            seed = seed,
            lhefile = '/workdir/output_{}.lhe'.format(index)
        )
        self.addStep(mad.s(**arguments))

@stage.fromfunc(wflow, name = 'delphes', after = 'madgraph')
def pythia_delphes(self):
    for index,hepmcfile in enumerate(utils.regex_match_outputs([self.workflow.element('madgraph')],['lhefile'])):
        delphes = yadagestep('pythiadelphes_{}'.format(index),steps['pythia_delphes'],self.context)
        inputname = utils.read_input(self.dag,delphes,hepmcfile)
        arguments = dict(
            pythia_card = '/analysis/mainPythiaMLM.cmnd',
            detector_card = '/analysis/template_cards/modified_delphes_card_ATLAS.tcl',
            lhefile = inputname,
            outputroot = '/workdir/delphesout_{}.root'.format(index)
        )
        self.addStep(delphes.s(**arguments))

@stage.fromfunc(wflow, name = 'analysis', after = 'delphes')
def post(self):
    for index,rootfile in enumerate(utils.regex_match_outputs([self.workflow.element('delphes')],['delphesoutput'])):

        analysis = yadagestep('analysis_{}'.format(index),steps['analysis'],self.context)
        inputname = utils.read_input(self.dag,analysis,rootfile)
        arguments = dict(
            fromdelphes = inputname,
            analysis_output = '/workdir/postout_{}.root'.format(index)
        )
        self.addStep(analysis.s(**arguments))

@stage.fromfunc(wflow, name = 'merge', after = 'analysis')
def merge(self):
    merge = yadagestep('merge',steps['rootmerge'],self.context)
    inputs = []
    for rootfile in utils.regex_match_outputs([self.workflow.element('analysis')],['analysis_output']):
        inputs += [utils.read_input(self.dag,merge,rootfile)]

    arguments = dict(
        mergedfile = '/workdir/analysis.merged.root',
        inputfiles = inputs
    )
    self.addStep(merge.s(**arguments))


workdir = '/workdir'
wflow.context = {
    'workdir':workdir,
    'init':{
        'seeds':[1234,5678,2345,6789],
        'couplingA':0.5,
        'couplingB':0.3
    }
}

the_dag = adage.mk_dag()
allstages = [s for s in  wflow.walk(recurse = True)]
adage.rundag(the_dag, backend = adage.backends.MultiProcBackend(10), rules = allstages, track = True)
write_prov_graph(workdir,the_dag)
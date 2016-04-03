import logging
import os
import yaml

import adage
from adage import Rule,functorize

import yadage.prototyping as proto
from yadage.yadagestep import yadagestep, initstep
from yadage.visualize import write_prov_graph
import yadage.handlers.utils as utils

logging.basicConfig(level = logging.INFO)
context = {
    'workdir':'/workdir',
    'init':{
        'seeds':[1234,5678,4567],
        'couplingA':0.5,
        'couplingB':0.3
    }
}

dag,rules = adage.mk_dag(),[]
workflow = {'stages':[]}

def stage(name):
    """
    get or create stage
    """
    select = [x for x in workflow['stages'] if x['name']==name]
    assert len(select) in [0,1]
    if len(select):
        return select[0]
    else:
        workflow['stages']+=[{'name':name}]
        return workflow['stages'][-1]

def stages():
    return {s['name']:s for s in workflow['stages']}

def step(stage,stepindex):
    return stage['scheduled_steps'][stepindex].identifier


class stage(object):
    def 

class workflow(object):
    def __init__(self):
        self.stages = {}
        
    def run(self,after = []):
        """
        returns a decorator that decorates a function body
        with a predicate that is true when the stages in 'after'
        have executed
        """
        def decorator(func):
        {
            stagenames = after if type(after)==list else [after]
        }
            self.rules += [Rule(all_good.s(names = stagenames),func)]
            return func
        return decorator


class steering(object):
    def __init__(self):
        self.rules = []
        self.dag = adage.mk_dag()

    
    def run(self,after = []):
        """
        returns a decorator that decorates a function body
        with a predicate that is true when the stages in 'after'
        have executed
        """
        def decorator(func):
            stagenames = after if type(after)==list else [after]
            self.rules += [Rule(all_good.s(names = stagenames),func)]
            return func
        return decorator
    
    def execute(self):
        adage.rundag(self.dag, rules = self.rules, track = True)


steps = yaml.load(open('steps.yml'))


my_workflow  = steering()

@my_workflow.run()
def init(dag):
    init = initstep(context['init'])
    utils.addStep(stage('init'),dag,init)

@my_workflow.run(after = 'init')
def prepare(dag):

    prep = yadagestep('prepare',steps['prepare'],context)

    par1 = utils.read_input(dag,prep,(step(stage('init'),0),'couplingA',None))
    par2 = utils.read_input(dag,prep,(step(stage('init'),0),'couplingB',None))
    
    arguments = dict(
        par1 = par1, par2 = par2, param_card = '/workdir/param_card.dat'
    )
    utils.addStep(stage('prepare'),dag,prep.s(**arguments))

@my_workflow.run(after = 'prepare')
def grid(dag):

    grid = yadagestep('grid',steps['grid'],context)
    paramcard = utils.read_input(dag,grid,(step(stage('prepare'),0),'param_card',None))
    arguments = dict(
        param_card = paramcard,
        gridpack ='/workdir/grid.tar.gz'
    )

    utils.addStep(stage('grid'),dag,grid.s(**arguments))

@my_workflow.run(after = 'grid')
def schedule_madgraph(dag):
    gridpack_ref = (step(stage('grid'),0),'gridpack',None)
    for index,seed_ref in enumerate(utils.regex_match_outputs([stage('init')],['seeds'])):

        mad = yadagestep('madgraph {}'.format(index),steps['madgraph'],context)
        gridpack = utils.read_input(dag,mad,gridpack_ref)
        seed     = utils.read_input(dag,mad,seed_ref)

        arguments = dict(
            gridpack = gridpack,
            nevents = 1000,
            seed = seed,
            lhefile = '/workdir/output_{}.lhe'.format(index)
        )
        utils.addStep(stage('madgraph'),dag,mad.s(**arguments))

@my_workflow.run(after = 'madgraph')
def schedule_pythia(dag):
    for index,lhefile in enumerate(utils.regex_match_outputs([stage('madgraph')],['lhefile'])):

        pythia = yadagestep('pythia {}'.format(index),steps['pythia'],context)
        inputname = utils.read_input(dag,pythia,lhefile)
        arguments = dict(
            settings_file = '/analysis/mainPythiaMLM.cmnd',
            hepmcfile = '/workdir/output_{}.hepmc'.format(index),
            lhefile = inputname
        )

        utils.addStep(stage('pythia'),dag,pythia.s(**arguments))

@my_workflow.run(after = 'pythia')
def schedule_delphes(dag):
    for index,hepmcfile in enumerate(utils.regex_match_outputs([stage('pythia')],['hepmcfile'])):

        delphes = yadagestep('delphes {}'.format(index),steps['delphes'],context)
        inputname = utils.read_input(dag,delphes,hepmcfile)
        arguments = dict(
            detector_card = '/analysis/template_cards/modified_delphes_card_ATLAS.tcl',
            inputfile = inputname,
            outputfile = '/workdir/delphesout_{}.root'.format(index)
        )
        utils.addStep(stage('delphes'),dag,delphes.s(**arguments))

@my_workflow.run(after = 'delphes')
def schedule_post(dag):
    for index,rootfile in enumerate(utils.regex_match_outputs([stage('delphes')],['delphesoutput'])):

        analysis = yadagestep('analysis {}'.format(index),steps['analysis'],context)
        inputname = utils.read_input(dag,analysis,rootfile)
        arguments = dict(
            fromdelphes = inputname,
            analysis_output = '/workdir/postout_{}.root'.format(index)
        )
        utils.addStep(stage('post'),dag,analysis.s(**arguments))

@my_workflow.run(after = 'post')
def schedule_merge(dag):

    merge = yadagestep('merge',steps['rootmerge'],context)
    inputs = []
    for rootfile in utils.regex_match_outputs([stage('post')],['analysis_output']):
        inputs += [utils.read_input(dag,merge,rootfile)]

    arguments = dict(
        mergedfile = '/workdir/analysis.merged.root',
        inputfiles = inputs
    )
    utils.addStep(stage('post'),dag,merge.s(**arguments))

my_workflow.execute()
write_prov_graph(os.getcwd(),my_workflow.dag,workflow)
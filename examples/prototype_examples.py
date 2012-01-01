from yadage.prototyping import stage
from yadage.yadagestep  import yadagestep
import yadage.yadagemodels
import yadage.visualize
import yadage.handlers.utils as utils
import logging
import yaml
import adage
import os

logging.basicConfig(level = logging.DEBUG)
stepdata = yaml.load(open('prototpye_steps.yml'))

flow = yadage.yadagemodels.workflow()


adageobj = adage.adageobject()

context = {
    'hello':'what',
    'seeds':[1,2,3]
}

@stage.fromfunc(flow)
def prepare(stage):
    step = yadagestep('prepare',stepdata['prepare'],stage.context)
    
    arguments = dict(
        par1 = 0.5, par2 = 0.3, param_card = '/workdir/param_card.dat'
    )
    stage.addStep(step.s(**arguments))
    
# @stage.fromfunc(flow, after = ['prepare'])
# def grid(stage):
#
#     grid = yadagestep('grid',stepdata['grid'],stage.context)
#
#     prepstep = stage.workflow.stage('prepare').scheduled_steps[0]
#     paramcard = prepstep.result['param_card']
#
#     paramcard = grid.used_input(prepstep.identifier,'param_card',None)
#     arguments = dict(
#         param_card = paramcard,
#         gridpack ='/workdir/grid.tar.gz'
#     )
#     stage.addStep(grid.s(**arguments))
#
# @stage.fromfunc(flow, after = ['grid'])
# def madgraph(stage):
#     gridpackstep = stage.workflow.stage('grid').scheduled_steps[0]
#     gridpack_ref = (gridpackstep.identifier,'gridpack',None)
#     gridpack     = gridpackstep.result['gridpack']
#
#     for index,seed in enumerate(stage.context['seeds']):
#
#         mad      = yadagestep('madgraph {}'.format(index),stepdata['madgraph'],stage.context)
#         gridpack = mad.used_input(*gridpack_ref)
#         arguments = dict(
#             gridpack = gridpack,
#             nevents = 1000,
#             seed = seed,
#             lhefile = '/workdir/output_{}.lhe'.format(index)
#         )
#         stage.addStep(mad.s(**arguments))


adageobj = adage.adageobject()
adageobj.rules = flow.stages.values()
adage.rundag(adageobj, track = True)
yadage.visualize.write_prov_graph(os.getcwd(),adageobj.dag,flow)
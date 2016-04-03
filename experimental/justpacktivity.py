from packtivity import packtivity
import yaml
import logging
logging.basicConfig(level = logging.DEBUG)

steps = yaml.load(open('steps.yml'))
ctx = {'workdir':'bla'}


def runpack(step,**arguments):
    return packtivity(step,steps[step],arguments,ctx)

print runpack('prepare',par1 = 0.5, par2 = 0.3, param_card = '/workdir/param_card.dat')
print runpack('grid', param_card = '/workdir/param_card.dat', gridpack ='/workdir/grid.tar.gz')
print runpack('madgraph', nevents = 1000, seed = 1234, lhefile = '/workdir/output.lhe')
print runpack('pythia', settings_file = '/analysis/mainPythiaMLM.cmnd', hepmcfile = '/workdir/output.hepmc', lhefile = '/workdir/output.lhe')
print runpack('delphes', detector_card = '/analysis/template_cards/modified_delphes_card_ATLAS.tcl', outputfile = '/workdir/output.root', inputfile = '/workdir/output.hepmc')
print runpack('analysis', fromdelphes = '/workdir/output.root', analysis_output = '/workdir/analysis.root')




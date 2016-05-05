#!/bin/sh

#this is a test that actually runs the dilepton workflow to test out grid access and cvmfs access

mkdir /workdir/inputs
cd /workdir/inputs
curl http://physics.nyu.edu/~lh1132/recast_zara/255123.zip -O
unzip 255123.zip
yadage-run -t from-github /workdir ewk_analyses/ewkdilepton_analysis/ewk_dilepton_recast_workflow.yml /workdir/inputs/input.yaml

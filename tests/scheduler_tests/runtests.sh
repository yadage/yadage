#!/bin/sh
set -e
set -x
mkdir workdir
yadage-run -t scheduler-examples workdir map-from-ctx-workflow.yml mapctx.yml

rm -rf workdir/*
yadage-run -t scheduler-examples workdir map-from-dep-workflow.yml

rm -rf workdir/*
yadage-run -t scheduler-examples workdir reduce-from-dep-workflow.yml

rm -rf workdir/*
yadage-run -t scheduler-examples workdir single-from-ctx-workflow.yml singlefromctx.yml

rm -rf workdir/*
yadage-run -t scheduler-examples workdir zip-from-dep-workflow.yml

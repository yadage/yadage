#!/bin/sh
set -e
set -x
# mkdir workdir_one
# yadage-run -t scheduler-examples workdir_one map-from-ctx-workflow.yml mapctx.yml

mkdir workdir_two
yadage-run -t scheduler-examples workdir_two map-from-dep-workflow.yml

mkdir workdir_three
yadage-run -t scheduler-examples workdir_three reduce-from-dep-workflow.yml

# mkdir workdir_four
# yadage-run -t scheduler-examples workdir_four single-from-ctx-workflow.yml singlefromctx.yml

mkdir workdir_five
yadage-run -t scheduler-examples workdir_five zip-from-dep-workflow.yml

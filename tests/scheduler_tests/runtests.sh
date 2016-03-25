#!/bin/sh
set -e
set -x
mkdir workdir_one
yadage-run -t scheduler-examples workdir_one map-from-ctx-workflow.yml mapctx.yml || echo 'ERROR one'

mkdir workdir_two
yadage-run -t scheduler-examples workdir_two map-from-dep-workflow.yml || echo 'ERROR two'

mkdir workdir_three
yadage-run -t scheduler-examples workdir_three reduce-from-dep-workflow.yml || echo 'ERROR three'

#mkdir workdir_four
#yadage-run -t scheduler-examples workdir_four single-from-ctx-workflow.yml singlefromctx.yml || echo 'ERROR four'

mkdir workdir_five
yadage-run -t scheduler-examples workdir_five zip-from-dep-workflow.yml || echo 'ERROR five'

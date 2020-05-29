# yadage - yaml based adage

[![arXiv](https://img.shields.io/badge/arXiv-1706.01878-orange.svg)](https://arxiv.org/abs/1706.01878)
[![DOI](https://zenodo.org/badge/53543066.svg)](https://zenodo.org/badge/latestdoi/53543066)
[![PyPI version](https://badge.fury.io/py/yadage.svg)](https://badge.fury.io/py/yadage)
[![GitHub Actions Status: CI](https://github.com/yadage/yadage/workflows/CI/CD/badge.svg)](https://github.com/yadage/yadage/actions?query=workflow%3ACI%2FCD+branch%3Amaster)
[![Code Coverage](https://codecov.io/gh/yadage/yadage/graph/badge.svg?branch=master)](https://codecov.io/gh/yadage/yadage?branch=master)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/yadage/yadage.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/yadage/yadage/latest/files/)
[![CodeFactor](https://www.codefactor.io/repository/github/yadage/yadage/badge)](https://www.codefactor.io/repository/github/yadage/yadage)
[![Documentation Status](https://readthedocs.org/projects/yadage/badge/?version=latest)](http://yadage.readthedocs.io/en/latest/?badge=latest)
[![](https://images.microbadger.com/badges/version/yadage/yadage.svg)](https://microbadger.com/images/yadage/yadage "Get your own version badge on microbadger.com")
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This package reads and executes workflows adhering to the workflow JSON schemas defined at https://github.com/yadage/yadage-schemas such as the ones stored in the community repository https://github.com/yadage/yadage-workflows. For executing the individual steps it mainly uses the packtivity python bindings provided by https://github.com/yadage/packtivity.

### Example Workflow

```
cat << 'EOF' > workflow.yml
stages:
- name: hello_world
  dependencies: [init]
  scheduler:
    scheduler_type: singlestep-stage
    parameters:
      name: {step: init, output: name}
      outputfile: '{workdir}/hello_world.txt'
    step:
      process:
        process_type: 'string-interpolated-cmd'
        cmd: 'echo Hello my Name is {name} | tee {outputfile}'
      publisher:
        publisher_type: 'frompar-pub'
        outputmap:
          outputfile: outputfile
      environment:
        environment_type: 'docker-encapsulated'
        image: busybox
EOF
```

You can try this workflow via

```
yadage-run -p name="John Doe"
```

For more thorough examples, please see the [documentation](http://yadage.readthedocs.io/en/latest/)

### Possible Backends:

Yadage can run on various backends such as multiprocessing pools, ipython clusters, or celery clusters. If human intervention is needed for certain steps, it can also be run interactively.

### Published versions of related packages (main dependencies of yadage)

| package | version |
| ------------- |-------------|
| packtivity     | [![PyPI version](https://badge.fury.io/py/packtivity.svg)](https://badge.fury.io/py/packtivity) |
| yadage-schemas    | [![PyPI version](https://badge.fury.io/py/yadage-schemas.svg)](https://badge.fury.io/py/yadage-schemas)      |   
| adage | [![PyPI version](https://badge.fury.io/py/adage.svg)](https://badge.fury.io/py/adage)      |  

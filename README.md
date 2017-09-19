# yadage - yaml based adage

[![arXiv](https://img.shields.io/badge/arXiv-1706.01878-orange.svg)](https://arxiv.org/abs/1706.01878)
[![DOI](https://zenodo.org/badge/53543066.svg)](https://zenodo.org/badge/latestdoi/53543066)
[![PyPI version](https://badge.fury.io/py/yadage.svg)](https://badge.fury.io/py/yadage)
[![Build Status](https://travis-ci.org/diana-hep/yadage.svg?branch=master)](https://travis-ci.org/diana-hep/yadage)
[![Code Health](https://landscape.io/github/diana-hep/yadage/master/landscape.svg?style=flat)](https://landscape.io/github/diana-hep/yadage/master)
[![Coverage Status](https://coveralls.io/repos/github/diana-hep/yadage/badge.svg?branch=master)](https://coveralls.io/github/diana-hep/yadage?branch=master)
[![Documentation Status](https://readthedocs.org/projects/yadage/badge/?version=latest)](http://yadage.readthedocs.io/en/latest/?badge=latest)
[![](https://images.microbadger.com/badges/version/yadage/yadage.svg)](https://microbadger.com/images/yadage/yadage "Get your own version badge on microbadger.com")


A declarative way to define [adage](https://github.com/diana-hep/adage.git) workflows using a JSON schema (but we'll always write it as YAML)

    docker run --rm -it -v /var/run/docker.sock:/var/run/docker.sock -v $PWD:$PWD -w $PWD lukasheinrich/yadage bash
    yadage-run -t from-github/phenochain mdwork madgraph_delphes.yml -p nevents=100

or just 

    eval "$(curl https://raw.githubusercontent.com/diana-hep/yadage/master/yadagedocker.sh)"
    yadage-run -t from-github/phenochain mdwork madgraph_delphes.yml -p nevents=100

This package reads and executes workflows adhering to the workflow JSON schemas defined at https://github.com/diana-hep/cap-schemas such as the onces stored in the community repository https://github.com/lukasheinrich/yadage-workflows. For executing the individual steps it mainly uses the packtivity python bindings provided by https://github.com/diana-hep/packtivity.

### Possible Backends:

Yadage can run on various backends such as multiprocessing pools, ipython clusters, or celery clusters. If human intervention is needed for certain steps, it can also be run interactively.

### Example Workflow

    stages:
      - name: hello_world
        dependencies: [init]
        scheduler:
          scheduler_type: singlestep-stage
          parameters:
            parone: {stages: init, output: par, unwrap: true}
            outputfile: '{workdir}/hello_world.txt'
          step:
            process:
              process_type: 'string-interpolated-cmd'
              cmd: 'echo Hello {parone} | tee {outputfile}'
            publisher:
              publisher_type: 'frompar-pub'
              outputmap:
                outputfile: outputfile
            environment:
              environment_type: 'docker-encapsulated'
              image: busybox
              
You can try this workflow via 

    yadage-run -t from-github/testing/busybox-helloworld workdir workflow.yml -p par=World

For more thorough examples, please see the [documentation](http://yadage.readthedocs.io/en/latest/)

### Published versions of related packages (main dependencies of yadage)

| package | version |
| ------------- |-------------| 
| packtivity     | [![PyPI version](https://badge.fury.io/py/packtivity.svg)](https://badge.fury.io/py/packtivity) | 
| yadage-schemas    | [![PyPI version](https://badge.fury.io/py/yadage-schemas.svg)](https://badge.fury.io/py/yadage-schemas)      |   
| adage | [![PyPI version](https://badge.fury.io/py/adage.svg)](https://badge.fury.io/py/adage)      |  


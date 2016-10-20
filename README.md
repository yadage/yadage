# yadage - yaml based adage

[![PyPI version](https://badge.fury.io/py/yadage.svg)](https://badge.fury.io/py/yadage)
[![Build Status](https://travis-ci.org/diana-hep/yadage.svg?branch=master)](https://travis-ci.org/diana-hep/yadage)
[![Code Health](https://landscape.io/github/diana-hep/yadage/master/landscape.svg?style=flat)](https://landscape.io/github/diana-hep/yadage/master)
[![Documentation Status](https://readthedocs.org/projects/pip/badge/?version=latest)](http://yadage.readthedocs.org/en/latest/?badge=latest)
[![](https://images.microbadger.com/badges/image/lukasheinrich/yadage.svg)](https://microbadger.com/images/lukasheinrich/yadage "Get your own image badge on microbadger.com")

A declarative way to define [adage](https://github.com/diana-hep/adage.git) workflows using a JSON schema (but we'll always write it as YAML)

    docker run --rm -it -v /var/run/docker.sock:/var/run/docker.sock -v $PWD:$PWD -w $PWD lukasheinrich/yadage bash
    yadage-run -t from-github/phenochain mdwork madgraph_delphes.yml -p nevents=100

or just 

    eval "$(curl https://raw.githubusercontent.com/diana-hep/yadage/master/yadagedocker.sh)"
    yadage-run -t from-github/phenochain mdwork madgraph_delphes.yml -p nevents=100

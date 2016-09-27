# yadage - yaml based adage

[![Build Status](https://travis-ci.org/lukasheinrich/yadage.svg?branch=master)](https://travis-ci.org/lukasheinrich/yadage)
[![Code Health](https://landscape.io/github/lukasheinrich/yadage/master/landscape.svg?style=flat)](https://landscape.io/github/lukasheinrich/yadage/master)
[![Documentation Status](https://readthedocs.org/projects/yadage/badge/?version=latest)](http://yadage.readthedocs.org/en/latest/?badge=latest)
[![](https://images.microbadger.com/badges/image/lukasheinrich/yadage.svg)](https://microbadger.com/images/lukasheinrich/yadage "Get your own image badge on microbadger.com")

A declarative way to define [adage](https://github.com/lukasheinrich/adage.git) workflows using a JSON schema (but we'll always write it as YAML)

    docker run --rm -it -v /var/run/docker.sock:/var/run/docker.sock -v $PWD:$PWD -w $PWD lukasheinrich/yadage bash
    yadage-run -t from-github/phenochain mdwork madgraph_delphes.yml -p nevents=100

or just 

    eval "$(curl https://raw.githubusercontent.com/lukasheinrich/yadage/master/yadagedocker.sh)"
    yadage-run -t from-github/phenochain mdwork madgraph_delphes.yml -p nevents=100

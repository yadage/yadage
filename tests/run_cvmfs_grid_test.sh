#!/bin/sh
docker volume create --name yadagetest
export YADAGEWORK=$(docker volume inspect yadagetest |grep Mou|awk '{print $NF}'|sed 's|"||g')
docker run -v /var/run/docker.sock:/var/run/docker.sock -e PACKTIVITY_WORKDIR_LOCATION=$YADAGEWORK -e PACKTIVITY_WITHIN_DOCKER=true -v yadagetest:/workdir yadage ./tests/cvmfs_grid_test.sh

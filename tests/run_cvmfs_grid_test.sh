#!/bin/sh
docker volume create --name yadagetest
export YADAGEWORK=$(docker volume inspect -f '{{.Mountpoint}}' yadagetest)
docker run -v /var/run/docker.sock:/var/run/docker.sock -e PACKTIVITY_WORKDIR_LOCATION="/workdir:$YADAGEWORK" -e PACKTIVITY_WITHIN_DOCKER=true -v yadagetest:/workdir yadage ./tests/cvmfs_grid_test.sh
#!/bin/bash
docker login -u "$DOCKERLOGIN" -p "$DOCKERPW"
docker build -t yadage/yadage:$TRAVIS_BRANCH .
docker push yadage/yadage:$TRAVIS_BRANCH


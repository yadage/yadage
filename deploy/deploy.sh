#!/bin/bash

docker login -u "$DOCKERLOGIN" -p "$DOCKERPW"
docker build -t yadage/yadage .
docker push yadage/yadage

function yadage-run {
   docker run --rm -it -e PACKTIVITY_DOCKER_NOPULL=true -v $PWD:$PWD -w $PWD -v /var/run/docker.sock:/var/run/docker.sock lukasheinrich/yadage yadage-run $*
}


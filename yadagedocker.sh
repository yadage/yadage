function yadage-run {
   docker run -it -v $PWD:$PWD -w $PWD -v /var/run/docker.sock:/var/run/docker.sock lukasheinrich/yadage yadage-run $*
}

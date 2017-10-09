FROM fedora
ARG VCS_REF=none
LABEL org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-url="https://github.com/diana-hep/yadage"
RUN dnf install -y gcc gcc-c++ graphviz-devel ImageMagick python-devel libffi-devel openssl openssl-devel unzip nano autoconf automake libtool redhat-rpm-config; dnf clean all 
COPY . /yadage
WORKDIR /yadage
RUN curl https://bootstrap.pypa.io/get-pip.py | python -
RUN curl https://get.docker.com/builds/Linux/x86_64/docker-1.9.1  -o /usr/bin/docker && chmod +x /usr/bin/docker
RUN pip install -e . --process-dependency-links

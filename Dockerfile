FROM cern/cc7-base
COPY . /yadage
WORKDIR /yadage
RUN curl https://bootstrap.pypa.io/get-pip.py | python -
RUN curl https://get.docker.com/builds/Linux/x86_64/docker-1.9.1  -o /usr/bin/docker && chmod +x /usr/bin/docker
RUN yum install -y gcc graphviz-devel ImageMagick python-devel
RUN pip install https://github.com/lukasheinrich/adage/archive/master.zip
RUN pip install -e .

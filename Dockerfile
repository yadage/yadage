FROM alpine
RUN  apk add automake autoconf libtool \
             python-dev musl-dev libffi-dev \
             python-dev musl-dev libffi-dev gcc \
             autoconf curl gcc ipset ipset-dev iptables iptables-dev libnfnetlink libnfnetlink-dev libnl3 libnl3-dev make musl-dev openssl openssl-dev \
             jq util-linux font-bitstream-type1 build-base graphviz-dev imagemagick graphviz
#pip
RUN curl https://bootstrap.pypa.io/get-pip.py | python -

#docker
RUN curl https://download.docker.com/linux/static/stable/x86_64/docker-18.03.1-ce.tgz|tar -xzvf - && \
    cp docker/docker /usr/local/bin && \
    rm -rf docker

#kubctl
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.12.0/bin/linux/amd64/kubectl && \
    mv kubectl /usr/local/bin && \
    chmod +x /usr/local/bin/kubectl

#yadage
RUN pip install pydotplus kubernetes
ADD . /code
RUN pip install -e /code

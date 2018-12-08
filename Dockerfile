FROM alpine
RUN  apk add automake autoconf libtool && \
     apk add python-dev musl-dev libffi-dev && \
     apk add python-dev musl-dev libffi-dev gcc && \
     apk add autoconf curl gcc ipset ipset-dev iptables iptables-dev libnfnetlink libnfnetlink-dev libnl3 libnl3-dev make musl-dev openssl openssl-dev && \
     curl https://bootstrap.pypa.io/get-pip.py | python -
RUN apk add graphviz-dev imagemagick graphviz
RUN curl https://download.docker.com/linux/static/stable/x86_64/docker-18.03.1-ce.tgz|tar -xzvf - && \
    cp docker/docker /usr/local/bin && \
    rm -rf docker
RUN pip install yadage[viz] pydotplus

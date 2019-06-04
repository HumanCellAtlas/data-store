# This is the build image for the DSS, intended for use with dss-monitor fargate cluster
# It may be built and uploaded with the commands:
#   `docker login
#   `docker build -f fargate.Dockerfile -t {docker_username}/{tag_key}:{tag_value} .`
#   `docker push {docker_username}/{tag_key}:{tag_value}`
# For example,
#   `docker login
#   `docker build -f fargate.Dockerfile -t humancellatlas/dss-monitor-image .`
#   `docker push humancellatlas/dss-monitor-image`
#
# Please see Docker startup guide for additional info:
#   https://docs.docker.com/get-started/

FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update --quiet \
    && apt-get install --assume-yes --no-install-recommends \
        build-essential \
        git \
        jq \
        make \
        moreutils \
        openssl \
        python3-pip \
        python3.6-dev \
        unzip \
        wget \
        xxd \
        zlib1g-dev \
        zip

RUN apt-get update --quiet
RUN python3 -m pip install --upgrade pip==10.0.1
RUN python3 -m pip install virtualenv==16.0.0
RUN ln -s /usr/bin/python3.6 /usr/bin/python
RUN ln -s /usr/bin/pip3 /usr/bin/pip

COPY [ "./entrypoint.sh", "/root/entrypoint.sh" ]

ENTRYPOINT /root/entrypoint.sh
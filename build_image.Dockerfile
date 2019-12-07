# This is the build image for the DSS, intended for use with the allspark GitLab server
# It may be built and uploaded with the commands:
#   `docker login
#   `docker build -f allspark.Dockerfile -t {docker_username}/{tag_key}:{tag_value} .`
#   `docker push {docker_username}/{tag_key}:{tag_value}`
# For example,
#   `docker login
#   `docker build -f allspark.Dockerfile -t humancellatlas/dss-build-box .`
#   `docker push humancellatlas/dss-build-box`
#
# Now reference the image in .gitlab-ci.yml with the line:
#   `image: {docker_username}/{tag_key}:{tag_value}
#
# Please see Docker startup guide for additional info:
#   https://docs.docker.com/get-started/ 

FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update --quiet \
    && apt-get install --assume-yes --no-install-recommends \
        ca-certificates \
        build-essential \
        default-jre \
        gettext \
        git \
        httpie \
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

ENV USER_HOME ${USER_HOME}
RUN useradd -d ${USER_HOME} -ms /bin/bash -g root -G sudo hca_cicd
RUN mkdir /HumanCellAtlas && chown hca_cicd /HumanCellAtlas
USER hca_cicd
WORKDIR ${USER_HOME}

ENV PATH ${USER_HOME}/bin:${PATH}
RUN mkdir -p ${USER_HOME}/bin

ENV ES_VERSION 5.4.2
ENV DSS_TEST_ES_PATH=${USER_HOME}/elasticsearch-${ES_VERSION}/bin/elasticsearch
RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${ES_VERSION}.tar.gz \
    && tar -xzf elasticsearch-${ES_VERSION}.tar.gz -C ${USER_HOME}

ENV TF_VERSION 0.12.16
RUN wget https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_amd64.zip \
    && unzip terraform_${TF_VERSION}_linux_amd64.zip -d ${USER_HOME}/bin

# Address locale problem, see "Python 3 Surrogate Handling":
# http://click.pocoo.org/5/python3/
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

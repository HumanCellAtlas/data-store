# This is a build image for the DSS, its intended to be used in conjunction with
#  the docker image `humancellatlas/dss-build-box`
# This image builds out dependencies that are required for the DSS as layers
#
# We attempt to use the AWS image that the lambda's use for compatability, match
# the AMI version from https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html
# with what is provided on docker.
#
# It may be built and uploaded with the commands:
#   `docker login
#   `docker build -f allspark.Dockerfile -t {docker_username}/{tag_key}:{tag_value} .`
#   `docker push {docker_username}/{tag_key}:{tag_value}`
# For example,
#   `docker login
#   `docker build -f lambda-layer.Dockerfile -t humancellatlas/dss-lambda-layer .`
#   `docker push humancellatlas/dss-lambda-layer`
#
# Now reference the image in .gitlab-ci.yml with the line:
#   `image: {docker_username}/{tag_key}:{tag_value}


FROM amazonlinux:2017.03

RUN yum -y install git \
    python36 \
    python36-pip \
    zip \
    && yum clean all

RUN python3 -m pip install --upgrade pip \
    && python3 -m pip install boto3 \
    && python3 -m pip install virtualenv

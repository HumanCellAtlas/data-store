#!/bin/bash

# This script sets the version variable DSS_VERSION into the SSM parameter contianing
# the environment variables used when deploying lambdas, and into all deployed lambdas

set -euo pipefail

if [[ -z $DSS_DEPLOYMENT_STAGE ]]; then
    echo 'Please run "source environment" in the data-store repo root directory before running this command'
    exit 1
fi

if [[ $DSS_DEPLOYMENT_STAGE == dev ]]; then
    version=$(git rev-parse HEAD)
elif [[ "$(git tag --points-at HEAD)" != "" ]]; then
    version=$(git tag --points-at HEAD | tail -n 1)
else
    version=$(git describe --tags --always)
fi

echo ${version} | scripts/dss-ops.py lambda set --quiet DSS_VERSION

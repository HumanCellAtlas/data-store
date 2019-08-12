#!/bin/bash

set -euo pipefail

if [[ $# != 1 ]]; then
    echo "Usage: $(basename $0) daemon-name"
    exit 1
fi

if [[ -z $DSS_DEPLOYMENT_STAGE ]]; then
    echo 'Please run "source environment" in the data-store repo root directory before running this command'
    exit 1
fi

daemon=$1

echo "Running pre-packaging steps for $daemon"

git clean -df $daemon/domovoilib $daemon/vendor

shopt -s nullglob
for wheel in $daemon/vendor.in/*/*.whl; do
    unzip -q -o -d $daemon/vendor $wheel
done

cp -R ../dss ../dss-api.yml $daemon/domovoilib
aws secretsmanager get-secret-value --secret-id ${DSS_SECRETS_STORE}/${DSS_DEPLOYMENT_STAGE}/gcp-credentials.json \
    | jq -r .SecretString > $daemon/domovoilib/gcp-credentials.json

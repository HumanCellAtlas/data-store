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

if [[ $daemon == "dss-scalability-test" ]]; then
    mkdir -p $daemon/domovoilib/tests
    $DSS_HOME/scripts/deploy_scale_dashboard.py
    $DSS_HOME/scripts/deploy_scale_tables.py
fi

cp "$GOOGLE_APPLICATION_CREDENTIALS" $daemon/domovoilib/gcp-credentials.json
chmod -R ugo+rX $daemon/domovoilib

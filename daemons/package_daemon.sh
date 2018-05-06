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
../scripts/get_dss_secret.py gcp-credentials.json $daemon/domovoilib/gcp-credentials.json

# Add service account email to list of authorized emails for ci-cd testing.
service_account_email=`jq -r ".client_email" $daemon/domovoilib/gcp-credentials.json`
admin_user_emails_length=${#ADMIN_USER_EMAILS}
if $admin_user_emails_length>0; then
	export ADMIN_USER_EMAILS="${ADMIN_USER_EMAILS},${service_account_email}"
else
	export ADMIN_USER_EMAILS="${service_account_email}"
fi

chmod -R ugo+rX $daemon/domovoilib

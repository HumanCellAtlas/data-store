#!/bin/bash

source "$(dirname $0)/../environment"

set -euo pipefail

if [[ $# != 1 ]]; then
    echo "Usage: $(basename $0) iam-principal-arn"
    exit 1
fi

export iam_principal_arn=$1
policy_json="$(dirname $0)/../iam/policies/ci-cd.json"
export region_name=$(aws configure get region)
export account_id=$(aws sts get-caller-identity | jq -r .Account)

# TODO: (akislyuk) finish this script

user_name=$iam_principal_arn # FIXME
aws iam attach-user-policy --user-name $user_name --policy-arn $<(cat "$policy_json" | envsubst '$DSS_S3_TEST_BUCKET $account_id $region_name')

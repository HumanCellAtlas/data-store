#!/bin/bash

set -euo pipefail

if [[ $# != 2 ]]; then
    echo "Usage: $(basename $0) daemon-name stage"
    exit 1
fi

export daemon_name=$1 stage=$2
export lambda_name="${daemon_name}-${stage}" iam_role_name="${daemon_name}-${stage}"
deployed_json="$(dirname $0)/${daemon_name}/.chalice/deployed.json"
config_json="$(dirname $0)/${daemon_name}/.chalice/config.json"
policy_json="$(dirname $0)/${daemon_name}/.chalice/policy.json"
stage_policy_json="$(dirname $0)/${daemon_name}/.chalice/policy-${stage}.json"
policy_template="$(dirname $0)/../iam/policy-templates/${daemon_name}-lambda.json"
export region_name=$(aws configure get region)
export account_id=$(aws sts get-caller-identity | jq -r .Account)

dss_es_domain=${DSS_ES_DOMAIN:-dss-index-$stage}
if ! aws es describe-elasticsearch-domain --domain-name $dss_es_domain; then
    echo "Please create AWS elasticsearch domain $dss_es_domain or set DSS_ES_DOMAIN to an existing domain and try again"
    exit 1
fi
export DSS_ES_ENDPOINT=$(aws es describe-elasticsearch-domain --domain-name "$dss_es_domain" | jq -r .DomainStatus.Endpoint)

cat "$config_json" | jq ".stages.$stage.api_gateway_stage=env.stage" | sponge "$config_json"

export lambda_arn=$(aws lambda list-functions | jq -r '.Functions[] | select(.FunctionName==env.lambda_name) | .FunctionArn')
if [[ -z $lambda_arn ]]; then
    echo "Lambda function $lambda_name not found, resetting deploy config"
    rm -f "$deployed_json"
else
    cat "$deployed_json" | jq .$stage.api_handler_arn=env.lambda_arn | sponge "$deployed_json"
fi

for var in $EXPORT_ENV_VARS_TO_LAMBDA; do
    cat "$config_json" | jq .stages.$stage.environment_variables.$var=env.$var | sponge "$config_json"
done

if [[ ${CI:-} == true ]]; then
    export iam_role_arn=$(aws iam list-roles | jq -r '.Roles[] | select(.RoleName==env.iam_role_name) | .Arn')
    cat "$config_json" | jq .manage_iam_role=false | jq .iam_role_arn=env.iam_role_arn | sponge "$config_json"
fi

cat "$policy_template" | envsubst '$DSS_S3_BUCKET_TEST $account_id $stage $region_name' > "$policy_json"
cp "$policy_json" "$stage_policy_json"

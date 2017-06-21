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

export lambda_arn=$(aws lambda list-functions | jq -r '.Functions[] | select(.FunctionName==env.lambda_name) | .FunctionArn')
if [[ -z $lambda_arn ]]; then
    echo "Lambda function $lambda_name not found, resetting deploy config"
    rm -f "$deployed_json"
else
    cat "$deployed_json" | jq .$stage.api_handler_arn=env.lambda_arn | sponge "$deployed_json"
fi

export DSS_ES_ENDPOINT=$(aws es describe-elasticsearch-domain --domain-name dss-index-$stage | jq -r .DomainStatus.Endpoint)

for var in $EXPORT_ENV_VARS_TO_LAMBDA; do
    cat "$config_json" | jq .stages.$stage.environment_variables.$var=env.$var | sponge "$config_json"
done

if [[ ${CI:-} == true ]]; then
    export iam_role_arn=$(aws iam list-roles | jq -r '.Roles[] | select(.RoleName==env.iam_role_name) | .Arn')
    cat "$config_json" | jq .manage_iam_role=false | jq .iam_role_arn=env.iam_role_arn | sponge "$config_json"
fi

cat "${policy_json}.template" | envsubst '$DSS_S3_TEST_BUCKET' > "$policy_json"

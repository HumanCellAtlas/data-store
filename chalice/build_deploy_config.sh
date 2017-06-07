#!/bin/bash

set -euo pipefail

get_api_id() {
    for api_id in $(aws apigateway get-rest-apis | jq -r .items[].id); do
        for resource_id in $(aws apigateway get-resources --rest-api-id $api_id | jq -r .items[].id); do
            aws apigateway get-integration --rest-api-id $api_id --resource-id $resource_id --http-method GET >/dev/null 2>&1 || continue
            uri=$(aws apigateway get-integration --rest-api-id $api_id --resource-id $resource_id --http-method GET | jq -r .uri)
            if [[ $uri == *"$lambda_arn"* ]]; then
                echo $api_id
                return
            fi
        done
    done
}

if [[ $# != 1 ]]; then
    echo "Usage: $(basename $0) apigateway-stage"
    exit 1
fi

stage=$1
deployed_json="$(dirname $0)/.chalice/deployed.json"
config_json="$(dirname $0)/.chalice/config.json"
export lambda_name=$(jq -r .$stage.api_handler_name "$deployed_json")

if [[ $lambda_name == "null" ]]; then
    echo "Invalid stage $stage"
    exit 1
fi

export lambda_arn=$(aws lambda list-functions | jq -r '.Functions[] | select(.FunctionName==env.lambda_name) | .FunctionArn')
if [[ -z $lambda_arn ]]; then
    echo "Lambda function $lambda_name not found, resetting Chalice config"
    rm -f "$deployed_json"
else
    export api_id=$(get_api_id)
    cat "$deployed_json" | jq .$stage.api_handler_arn=env.lambda_arn | jq .$stage.rest_api_id=env.api_id | sponge "$deployed_json"
fi

for var in $EXPORT_ENV_VARS_TO_LAMBDA; do
    cat "$config_json" | jq .stages.$stage.environment_variables.$var=env.$var | sponge "$config_json"
done

if [[ ${CI:-} == true ]]; then
    cat "$config_json" | jq .manage_iam_role=false | jq .iam_role_arn=env.chalice_lambda_iam_role_arn | sponge "$config_json"
fi

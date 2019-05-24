#!/bin/bash

set -euo pipefail

if [[ -z $FUS_DEPLOYMENT_STAGE ]]; then
    echo 'Please run "source environment" in the data-store repo root directory before running this command'
    exit 1
fi

export stage=$FUS_DEPLOYMENT_STAGE
deployed_json="$(dirname $0)/.chalice/deployed.json"
config_template_json="$(dirname $0)/.chalice/config_template.json"
config_json="$(dirname $0)/.chalice/config.json"
cp $config_template_json $config_json
policy_json="$(dirname $0)/.chalice/policy.json"
stage_policy_json="$(dirname $0)/.chalice/policy-${stage}.json"
export app_name=$(cat "$config_json" | jq -r .app_name)
iam_policy_template="$(dirname $0)/../iam/policy-templates/${app_name}-lambda.json"
export lambda_name="${app_name}-${stage}"
export account_id=$(aws sts get-caller-identity | jq -r .Account)

export lambda_arn=$(aws lambda list-functions | jq -r '.Functions[] | select(.FunctionName==env.lambda_name) | .FunctionArn')
if [[ -z $lambda_arn ]]; then
    echo "Lambda function $lambda_name not found, resetting Chalice config"
    rm -f "$deployed_json"
else
    api_arn=$(aws lambda get-policy --function-name "$lambda_name" | jq -r .Policy | jq -r '.Statement[0].Condition.ArnLike["AWS:SourceArn"]')
    export api_id=$(echo "$api_arn" | cut -d ':' -f 6 | cut -d '/' -f 1)
    jq -n ".$stage.api_handler_name = env.lambda_name | \
           .$stage.api_handler_arn = env.lambda_arn | \
           .$stage.rest_api_id = env.api_id | \
           .$stage.region = env.AWS_DEFAULT_REGION | \
           .$stage.api_gateway_stage = env.stage | \
           .$stage.backend = \"api\" | \
           .$stage.chalice_version = \"$(chalice --version | cut -f 2 -d ' ')\" | \
           .$stage.lambda_functions = {}" > "$deployed_json"
fi

export DEPLOY_ORIGIN="$(whoami)-$(hostname)-$(git describe --tags --always)-$(date -u +'%Y-%m-%d-%H-%M-%S').deploy"
export Name=fusillade-api-$stage
cat "$config_json" | jq ".stages.$stage.tags.FUS_DEPLOY_ORIGIN=env.DEPLOY_ORIGIN | \
                         .stages.$stage.tags.project=env.FUS_PROJECT_TAG | \
                         .stages.$stage.tags.owner=env.FUS_OWNER_TAG | \
                         .stages.$stage.tags.env=env.stage | \
                         .stages.$stage.tags.Name=env.Name | \
                         .stages.$stage.api_gateway_stage=env.stage" | sponge "$config_json"
env_json=$(aws ssm get-parameter --name /${FUS_PARAMETER_STORE}/${FUS_DEPLOYMENT_STAGE}/environment | jq -r .Parameter.Value)
for var in $(echo $env_json | jq -r keys[]); do
    val=$(echo $env_json | jq .$var)
    # TODO add version variable
    cat "$config_json" | jq .stages.$stage.environment_variables.$var="$val" | sponge "$config_json"
done

cp "$policy_json" "$stage_policy_json"
export secret_arn=$(aws secretsmanager describe-secret --secret-id ${FUS_SECRETS_STORE}/${FUS_DEPLOYMENT_STAGE}/oauth2_config | jq .ARN)
cat "$stage_policy_json" | jq .Statement[2].Resource[0]=$secret_arn | sponge "$stage_policy_json"

#!/bin/bash

set -euo pipefail

if [[ -z $DSS_DEPLOYMENT_STAGE ]]; then
    echo 'Please run "source environment" in the data-store repo root directory before running this command'
    exit 1
fi

export stage=$DSS_DEPLOYMENT_STAGE
deployed_json="$(dirname $0)/.chalice/deployed.json"
config_json="$(dirname $0)/.chalice/config.json"
policy_json="$(dirname $0)/.chalice/policy.json"
stage_policy_json="$(dirname $0)/.chalice/policy-${stage}.json"
export app_name=$(cat "$config_json" | jq -r .app_name)
export app_tag=$(echo $app_name | cut -d '-' -f2-)
iam_policy_template="$(dirname $0)/../iam/policy-templates/${app_name}-lambda.json"
export lambda_name="${app_name}-${stage}"
export account_id=$(aws sts get-caller-identity | jq -r .Account)

export dss_es_domain=${DSS_ES_DOMAIN}
if ! aws es describe-elasticsearch-domain --domain-name $dss_es_domain; then
    echo "Please create AWS elasticsearch domain $dss_es_domain or set DSS_ES_DOMAIN to an existing domain and try again"
    exit 1
fi

cat "$config_json" | jq ".stages.$stage.api_gateway_stage=env.stage" | sponge "$config_json"

export layer_name=dss-dependencies-${stage}
export layer_version_arn=$(aws lambda list-layers | jq -r '.Layers[] | select(.LayerName == env.layer_name) | .LatestMatchingVersion.LayerVersionArn')
cat "$config_json" | jq ".stages.$stage.layers=[env.layer_version_arn]" | sponge "$config_json"

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
           .$stage.chalice_version = \"1.0.1\" | \
           .$stage.lambda_functions = {}" > "$deployed_json"
fi

export DEPLOY_ORIGIN="$(whoami)-$(hostname)-$(git describe --tags --always)-$(date -u +'%Y-%m-%d-%H-%M-%S').deploy"
cat "$config_json" | jq ".stages.$stage.tags.DSS_DEPLOY_ORIGIN=\"$DEPLOY_ORIGIN\" | \
	.stages.$stage.tags.Name=\"${DSS_INFRA_TAG_SERVICE}-$app_name-${DSS_DEPLOYMENT_STAGE}\" | \
	.stages.$stage.tags.service=\"${DSS_INFRA_TAG_SERVICE}\"  | \
	.stages.$stage.tags.project=\"$DSS_INFRA_TAG_PROJECT\" | \
	.stages.$stage.tags.owner=\"${DSS_INFRA_TAG_OWNER}\" | \
	.stages.$stage.tags.env=\"${DSS_DEPLOYMENT_STAGE}\""  | sponge "$config_json"

env_json=$(aws ssm get-parameter --name /dcp/dss/${DSS_DEPLOYMENT_STAGE}/environment | jq -r .Parameter.Value)
for var in $(echo $env_json | jq -r keys[]); do
    val=$(echo $env_json | jq .$var)
    cat "$config_json" | jq .stages.$stage.environment_variables.$var="$val" | sponge "$config_json"
done

if [[ ${CI:-} == true ]]; then
    account_id=$(aws sts get-caller-identity | jq -r .Account)
    export iam_role_arn="arn:aws:iam::${account_id}:role/dss-${stage}"
    cat "$config_json" | jq .manage_iam_role=false | jq .iam_role_arn=env.iam_role_arn | sponge "$config_json"
fi

cat "$iam_policy_template" | envsubst '$DSS_S3_BUCKET $DSS_S3_CHECKOUT_BUCKET $dss_es_domain $account_id $stage' > "$policy_json"
cp "$policy_json" "$stage_policy_json"

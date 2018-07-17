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
iam_policy_template="$(dirname $0)/../iam/policy-templates/${app_name}-lambda.json"
export lambda_name="${app_name}-${stage}"
export account_id=$(aws sts get-caller-identity | jq -r .Account)

export dss_es_domain=${DSS_ES_DOMAIN}
if ! aws es describe-elasticsearch-domain --domain-name $dss_es_domain; then
    echo "Please create AWS elasticsearch domain $dss_es_domain or set DSS_ES_DOMAIN to an existing domain and try again"
    exit 1
fi
export DSS_VERSION=$(git describe --tags --always)
export DSS_ES_ENDPOINT=$(aws es describe-elasticsearch-domain --domain-name "$dss_es_domain" | jq -r .DomainStatus.Endpoint)
export EXPORT_ENV_VARS_TO_LAMBDA="$EXPORT_ENV_VARS_TO_LAMBDA DSS_ES_ENDPOINT DSS_VERSION"

cat "$config_json" | jq ".stages.$stage.api_gateway_stage=env.stage" | sponge "$config_json"

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
cat "$config_json" | jq .stages.$stage.tags.DSS_DEPLOY_ORIGIN=env.DEPLOY_ORIGIN | sponge "$config_json"

for var in $EXPORT_ENV_VARS_TO_LAMBDA; do
    cat "$config_json" | jq .stages.$stage.environment_variables.$var=env.$var | sponge "$config_json"
done

if [[ ${CI:-} == true ]]; then
    account_id=$(aws sts get-caller-identity | jq -r .Account)
    export iam_role_arn="arn:aws:iam::${account_id}:role/dss-${stage}"
    cat "$config_json" | jq .manage_iam_role=false | jq .iam_role_arn=env.iam_role_arn | sponge "$config_json"
fi

# Add service account email to list of authorized emails for ci-cd testing.
service_account_email=`jq -r ".client_email" chalicelib/gcp-credentials.json`
admin_user_emails_length=${#ADMIN_USER_EMAILS}
if [[ $admin_user_emails_length>0 ]]; then
	export ADMIN_USER_EMAILS="${ADMIN_USER_EMAILS},${service_account_email}"
else
	export ADMIN_USER_EMAILS="${service_account_email}"
fi

cat "$iam_policy_template" | envsubst '$DSS_S3_BUCKET $DSS_S3_CHECKOUT_BUCKET $dss_es_domain $account_id $stage' > "$policy_json"
cp "$policy_json" "$stage_policy_json"

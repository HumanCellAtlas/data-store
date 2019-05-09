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

export daemon_name=$1
export stage=$DSS_DEPLOYMENT_STAGE
export iam_role_name="${daemon_name}-${stage}"
config_json="$(dirname $0)/${daemon_name}/.chalice/config.json"
policy_json="$(dirname $0)/${daemon_name}/.chalice/policy.json"
stage_policy_json="$(dirname $0)/${daemon_name}/.chalice/policy-${stage}.json"
iam_policy_template=${iam_policy_template:-"$(dirname $0)/../iam/policy-templates/${daemon_name}-lambda.json"}
export account_id=$(aws sts get-caller-identity | jq -r .Account)
export region=$AWS_DEFAULT_REGION

export dss_es_domain=${DSS_ES_DOMAIN}
if ! aws es describe-elasticsearch-domain --domain-name $dss_es_domain; then
    echo "Please create AWS elasticsearch domain $dss_es_domain or set DSS_ES_DOMAIN to an existing domain and try again"
    exit 1
fi

cat "$config_json" | jq ".stages.$stage.api_gateway_stage=env.stage" | sponge "$config_json"

export layer_name=dss-dependencies-${stage}
export layer_version_arn=$(aws lambda list-layers | jq -r '.Layers[] | select(.LayerName == env.layer_name) | .LatestMatchingVersion.LayerVersionArn')
cat "$config_json" | jq ".stages.$stage.layers=[env.layer_version_arn]" | sponge "$config_json"


export DEPLOY_ORIGIN="$(whoami)-$(hostname)-$(git describe --tags --always)-$(date -u +'%Y-%m-%d-%H-%M-%S').deploy"
cat "$config_json" | jq .stages.$stage.tags.DSS_DEPLOY_ORIGIN=env.DEPLOY_ORIGIN | sponge "$config_json"

env_json=$(aws ssm get-parameter --name /dcp/dss/${DSS_DEPLOYMENT_STAGE}/environment | jq -r .Parameter.Value)
for var in $(echo $env_json | jq -r keys[]); do
    val=$(echo $env_json | jq .$var)
    cat "$config_json" | jq .stages.$stage.environment_variables.$var="$val" | sponge "$config_json"
done

if [[ ${CI:-} == true ]]; then
    export iam_role_arn=$(aws iam list-roles | jq -r '.Roles[] | select(.RoleName==env.iam_role_name) | .Arn')
    cat "$config_json" | jq .manage_iam_role=false | jq .iam_role_arn=env.iam_role_arn | sponge "$config_json"
fi

cat "$iam_policy_template" | envsubst '$DSS_S3_BUCKET $DSS_S3_BUCKET_TEST $DSS_S3_BUCKET_TEST_FIXTURES $DSS_S3_CHECKOUT_BUCKET $DSS_S3_CHECKOUT_BUCKET_TEST $DSS_S3_CHECKOUT_BUCKET_TEST_USER $dss_es_domain $account_id $stage' > "$policy_json"
cp "$policy_json" "$stage_policy_json"

if [[ $daemon_name == "dss-scalability-test" ]]; then
    $DSS_HOME/scripts/deploy_scale_dashboard.py
    $DSS_HOME/scripts/deploy_scale_tables.py
fi

if jq -e .dead_letter_queue_target_arn "$config_json"; then
    aws sqs create-queue --queue-name dss-dlq-${stage} --attributes DelaySeconds=5
    export reaper_sqs_arn="arn:aws:sqs:${region}:${account_id}:dss-dlq-${stage}"
    cat "$config_json" | jq ".dead_letter_queue_target_arn=env.reaper_sqs_arn" | sponge "$config_json"
fi

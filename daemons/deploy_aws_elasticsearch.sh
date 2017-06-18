#!/bin/bash

set -euo pipefail

function create_elasticsearch_domain() {
    local elasticsearch_domain_name=$1
    echo "Creating Elasticsearch domain with name: $elasticsearch_domain_name"
    # Just create a small ES cluster for now.
    # It is easy to increase the capacity as needed.
    elasticsearch_version="5.3" # Latest version supported by AWS as of 6/2017
    elasticsearch_cluster_config="InstanceType=t2.small.elasticsearch,InstanceCount=1"
    # 35GB is maximum allowed for a t2.small.elasticsearch
    ebs_options=EBSEnabled="true,VolumeType=gp2,VolumeSize=35"

    aws es create-elasticsearch-domain \
      --domain-name $elasticsearch_domain_name \
      --elasticsearch-version $elasticsearch_version \
      --elasticsearch-cluster-config $elasticsearch_cluster_config \
      --ebs-options $ebs_options
}

function update_elasticsearch_domain_policy() {
    local elasticsearch_domain_name=$1
    echo "Updating policy for Elasticsearch domain with name: $elasticsearch_domain_name"
    local directory=$(dirname $0)
    sed "s/ELASTICSEARCH_DOMAIN/$elasticsearch_domain_name/g" $directory/es_policy.json.template > $directory/es_policy.json
    aws es update-elasticsearch-domain-config --domain-name $elasticsearch_domain_name \
         --access-policies "$(< $directory/es_policy.json)"
}

function delete_elasticsearch_domain() {
    local elasticsearch_domain_name=$1
    echo "Deleting Elasticsearch domain with name: $elasticsearch_domain_name"
    aws es delete-elasticsearch-domain --domain-name $elasticsearch_domain_name
}

function check_elasticsearch_domain_exists() {
    local elasticsearch_domain_name=$1
    set +e # Don't terminate the program if grep doesn't to find a match.
    aws es list-domain-names | grep $elasticsearch_domain_name > /dev/null
    result=$?
    set +e
    return $result
}

function wait_for_elasticsearch_domain_removed() {
    local elasticsearch_domain_name=$1
    local timeout_value=$2
    local start_time=$(date +'%s')
    local timeout_expiration=$(($start_time + $timeout_value))

    echo "Waiting for Elasticsearch domain to be removed. Wait timeout: $((timeout_value / 60)) minutes ($timeout_value seconds) starting $(date)"
    while [[ $(date +'%s') -lt $timeout_expiration ]]; do
        check_elasticsearch_domain_exists $elasticsearch_domain_name
        if [[ $? -ne 0 ]]; then
            echo "Elasticsearch domain has been removed (actual wait $(( ($(date +'%s') - $start_time) / 60 )) minutes)"
            return 0
        fi
        echo "Waiting for Elasticsearch domain to be removed ..."
        sleep 5
    done
    echo "Timeout after waiting $((timeout_value / 60)) minutes for Elasticsearch domain to be removed"
    return 1 # Exit instead?
}

function wait_for_elasticsearch_endpoint() {
    local elasticsearch_domain_name=$1
    local timeout_value=$2
    local start_time=$(date +'%s')
    local timeout_expiration=$(($start_time + $timeout_value))

    echo "Waiting for Elasticsearch endpoint. Wait timeout: $((timeout_value / 60)) minutes ($timeout_value seconds) starting $(date)"
    while [[ $(date +'%s') -lt $timeout_expiration ]]; do
        result=$(get_elasticsearch_endpoint $elasticsearch_domain_name)
        if [[ -z "$result" ]] || [[ "$result" == "null" ]]; then
            echo "Waiting for Elasticsearch endpoint to be become available ..."
            sleep 5
        else
            echo "Elasticsearch endpoint is available (actual wait $(( ($(date +'%s') - $start_time) / 60 )) minutes)"
            return 0
        fi
    done
    echo "Timeout after waiting $((timeout_value / 60)) minutes for Elasticsearch endpoint"
    return 1 # Exit instead?
}

function get_elasticsearch_arn() {
    local elasticsearch_domain_name=$1
    check_elasticsearch_domain_exists $elasticsearch_domain_name &&
        aws es describe-elasticsearch-domain --domain-name $elasticsearch_domain_name 2> /dev/null \
            | jq -r '.DomainStatus.ARN'
}

function get_elasticsearch_endpoint() {
    local elasticsearch_domain_name=$1
    check_elasticsearch_domain_exists $elasticsearch_domain_name &&
        aws es describe-elasticsearch-domain --domain-name $elasticsearch_domain_name 2> /dev/null \
            | jq -r '.DomainStatus.Endpoint'
}

function set_elasticsearch_endpoint_in_chalice_config() {
    local elasticsearch_domain_name=$1
    local daemon_name=$2
    local stage=$3
    elasticsearch_endpoint=$(get_elasticsearch_endpoint $elasticsearch_domain_name)
    if [[ -n "$elasticsearch_endpoint" ]] && [[ "$elasticsearch_endpoint" == *amazonaws.com ]]; then
        var=DSS_ES_ENDPOINT
        export DSS_ES_ENDPOINT=$elasticsearch_endpoint
        config_json="$(dirname $0)/${daemon_name}/.chalice/config.json"
        cat "$config_json" | jq .stages.$stage.environment_variables.$var=env.$var | sponge "$config_json"
    else
        echo "Elasticsearch endpoint value is invalid: $elasticsearch_endpoint"
        echo Exiting
        exit 1
    fi
}

function display_elasticsearch_domain_info() {
    local elasticsearch_domain_name=$1
    elasticsearch_arn=$(get_elasticsearch_arn $elasticsearch_domain_name)
    echo "elasticsearch ARN: $elasticsearch_arn"

    elasticsearch_endpoint=$(get_elasticsearch_endpoint $elasticsearch_domain_name)
    echo "elasticsearch endpoint: $elasticsearch_endpoint"
}

function setup_elasticsearch_domain() {
    local elasticsearch_domain_name=$1
    local delete_existing_es_instance=$2
    if "$delete_existing_es_instance" == "True"; then
        check_elasticsearch_domain_exists $elasticsearch_domain_name
        if [[ $? -eq 0 ]] ; then
            # An elasticsearch domain with this name already exists
            echo "Elasticsearch domain already exists for $elasticsearch_domain_name"
            echo "Deleting existing Elasticsearch domain"
            delete_elasticsearch_domain $elasticsearch_domain_name

            wait_for_elasticsearch_domain_removed $elasticsearch_domain_name 900 # Up to 15 minutes
       fi
    fi

    check_elasticsearch_domain_exists $elasticsearch_domain_name
    if [[ $? -ne 0 ]]; then
        echo "Creating new Elasticsearch domain with name: $elasticsearch_domain_name"
        create_elasticsearch_domain $elasticsearch_domain_name

        # Should we only update the policy when the the elasticsearch domain is newly created
        # or always update the policy in case the policy has changed?
        echo "Setting/updating Elasticsearch policy"
        update_elasticsearch_domain_policy $elasticsearch_domain_name
    else
        echo "Using existing Elasticsearch domain with name: $elasticsearch_domain_name"
    fi
}

#
# Main
#

if [[ $# != 2 ]]; then
    echo "Usage: $(basename $0) daemon-name stage"
    exit 1
fi
declare -r global_daemon_name=$1
declare -r global_stage=$2
declare -r global_elasticsearch_domain_name="${global_daemon_name}-${global_stage}"
declare -r global_delete_existing_es_instance="False"

setup_elasticsearch_domain $global_elasticsearch_domain_name $global_delete_existing_es_instance

wait_for_elasticsearch_endpoint $global_elasticsearch_domain_name 1800 # Up to 30 minutes

# Add the Elasticsearch endpoint to the Chalice config.
# Because we plan to reuse the Elasticsearch domain across deployments
# the Elasticsearch endpoint should be stable and could be added
# to the Chalice configuration in the same way the other environment variables are.
set_elasticsearch_endpoint_in_chalice_config $global_elasticsearch_domain_name $global_daemon_name $global_stage

# Delete any existing Elasticsearch indexes
# There is no AWS CLI for this, so use python
echo "Removing all existing indexes in AWS Elasticsearch domain $global_elasticsearch_domain_name"
python $(dirname 0)/aws_elasticsearch_delete_index.py --domainname "$global_elasticsearch_domain_name" --index "_all"

display_elasticsearch_domain_info $global_elasticsearch_domain_name

echo "Completed Elasticsearch setup"

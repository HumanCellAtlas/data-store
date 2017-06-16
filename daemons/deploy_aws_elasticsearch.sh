#!/bin/bash

set -euo pipefail

function create_elasticsearch_domain() {
    echo Creating Elasticsearch domain with name: $elasticsearch_domain_name
    # Just create a small ES cluster for now.
    # It is easy to increase the capacity as needed.
    elasticsearch_version=5.3 # Latest version supported by AWS as of 6/2017
    elasticsearch_cluster_config=InstanceType=t2.small.elasticsearch,InstanceCount=1
    # 35GB is maximum allowed for a t2.small.elasticsearch
    ebs_options=EBSEnabled=true,VolumeType=gp2,VolumeSize=35

    aws es create-elasticsearch-domain \
      --domain-name $elasticsearch_domain_name \
      --elasticsearch-version $elasticsearch_version \
      --elasticsearch-cluster-config $elasticsearch_cluster_config \
      --ebs-options $ebs_options
}

function update_elasticsearch_domain_policy() {
    echo Updating policy for Elasticsearch domain with name: $elasticsearch_domain_name
    sed "s/ELASTICSEARCH_DOMAIN/$elasticsearch_domain_name/g" ./es_policy.json.template > ./es_policy.json
    aws es update-elasticsearch-domain-config --domain-name $elasticsearch_domain_name \
         --access-policies "$(< ./es_policy.json)"
}

function delete_elasticsearch_domain() {
    echo Deleting Elasticsearch domain with name: $elasticsearch_domain_name
    aws es delete-elasticsearch-domain --domain-name $elasticsearch_domain_name
}

function check_elasticsearch_domain_exists() {
    set +e
    aws es list-domain-names | grep $elasticsearch_domain_name > /dev/null
    result=$?
    set -e
    return $result
}

function wait_for_elasticsearch_domain_removed() {
	timeout_value=$1
	start_time=`date +'%s'`
	timeout_expiration=$(($start_time + $timeout_value))

	while [[ `date +'%s'` -lt $timeout_expiration ]]; do
	    check_elasticsearch_domain_exists
	    if [[ $? -ne 0 ]]; then
	        echo Elasticsearch domain has been removed
	        return 0
	    fi
        echo Waiting for Elasticsearch domain to be removed ...
		sleep 5
	done
	echo Timeout waiting for Elasticsearch domain to be removed
	return 1
}

function wait_for_elasticsearch_endpoint() {
	timeout_value=$1
	start_time=`date +'%s'`
	timeout_expiration=$(($start_time + $timeout_value))

	while [[ `date +'%s'` -lt $timeout_expiration ]]; do
	    if [[ `get_elasticsearch_endpoint $elasticsearch_domain_name` ]]; then
	        echo Elasticsearch endpoint is available.
	        return 0
	    fi
        echo Waiting for Elasticsearch endpoint to be become available ...
		sleep 5
	done
	echo Timeout waiting for Elasticsearch endpoint
	return 1
}

function get_elasticsearch_arn() {
    check_elasticsearch_domain_exists &&
    aws es describe-elasticsearch-domain --domain-name $elasticsearch_domain_name 2> /dev/null \
        | jq -r '.DomainStatus.ARN'
}

function get_elasticsearch_endpoint() {
    check_elasticsearch_domain_exists &&
    aws es describe-elasticsearch-domain --domain-name $elasticsearch_domain_name 2> /dev/null \
        | jq -r '.DomainStatus.Endpoint'
}

function display_elasticsearch_domain_info() {
    elasticsearch_arn=$(get_elasticsearch_arn $elasticsearch_domain_name)
    echo "elasticsearch ARN: " $elasticsearch_arn

    elasticsearch_endpoint=$(get_elasticsearch_endpoint $elasticsearch_domain_name)
    echo "elasticsearch endpoint: " $elasticsearch_endpoint
}

function setup_elasticsearch_domain() {
    if [[ $delete_existing_es_instance == True ]]; then
        check_elasticsearch_domain_exists
        if [[ $? -eq 0 ]] ; then
            # An elasticsearch domain with this name already exists

            echo Elasticsearch domain already exists for $elasticsearch_domain_name
            echo Deleting existing Elasticsearch domain
            delete_elasticsearch_domain $elasticsearch_domain_name

            wait_for_elasticsearch_domain_removed 300
       fi
    fi

    check_elasticsearch_domain_exists
    if [[ $? -ne 0 ]]; then
        echo Creating new Elasticsearch domain with name: $elasticsearch_domain_name
        create_elasticsearch_domain $elasticsearch_domain_name

        # Should we only update the policy when the the elasticsearch domain is newly created
        # or always update the policy in case the policy has changed?
        echo Setting/updating Elasticsearch policy
        update_elasticsearch_domain_policy $elasticsearch_domain_name
    else
        echo Using existing Elasticsearch domain with name: $elasticsearch_domain_name
    fi
}

#
# Main
#

if [[ $# != 2 ]]; then
    echo "Usage: $(basename $0) daemon-name stage"
    exit 1
fi
daemon_name=$1 stage=$2
elasticsearch_domain_name="${daemon_name}-${stage}"

delete_existing_es_instance=False

setup_elasticsearch_domain $elasticsearch_domain_name

wait_for_elasticsearch_endpoint 300

# TODO Delete existing indexes here via Python script

display_elasticsearch_domain_info

echo Completed Elasticsearch setup

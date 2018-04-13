#!/bin/bash

source "$(dirname $0)/../environment"

set -euo pipefail

if [[ $# != 1 ]]; then
    echo "$(basename $0): Launches an EC2 instance that deploys services"
    echo "when GitHub deployments are created."
    echo "Configures a GitHub deployment key and writes it to the"
    echo "AWS Secrets Manager for use by the builder instance."
    echo
    echo "Usage: $(basename $0) ec2-instance-name"
    exit 1
fi

builder_instance_name=$1

python -c 'import aegea.util.aws as aws; aws.ensure_instance_profile("dss-builder")'
aegea deploy grant git@github.com:HumanCellAtlas/data-store.git dss-builder
echo -n $GH_AUTH | aegea secrets put dss-deploy-github-token --iam-role dss-builder
cat gcp-credentials.json | aegea secrets put dss-gcp-credentials.json --iam-role dss-builder
cat application_secrets.json | aegea secrets put dss-application_secrets.json --iam-role dss-builder

echo "Builder AMI:"
if ! aegea images --json --tag AegeaMission=dss-builder | jq -e .[]; then
    echo "Builder AMI not found, building..."
    aegea-build-image-for-mission --image-type ami --mission-dir $DSS_HOME dss-builder dss-builder
fi

aegea launch $builder_instance_name --wait-for-ssh --ami-tags AegeaMission=dss-builder --iam-role dss-builder

#!/bin/bash

source "$(dirname $0)/../environment"

set -euo pipefail

python -c 'import aegea.util.aws as aws; aws.ensure_instance_profile("dss-builder")'
aegea deploy configure git@github.com:HumanCellAtlas/data-store.git dss-builder

echo "Builder AMI:"
if ! aegea images --json --tag AegeaMission=dss-builder | jq -e .[]; then
    echo "Builder AMI not found, building..."
    aegea-build-image-for-mission --image-type ami --mission-dir $DSS_HOME dss-builder dss-builder
fi

aegea launch dss-builder --wait-for-ssh --ami-tags AegeaMission=dss-builder

#Notes:
#- grab more install time scripting from travis ci (and possibly move it to makefile)
#- autopromote master->integration


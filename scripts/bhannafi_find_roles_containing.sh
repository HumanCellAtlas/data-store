#!/bin/bash

set -euo pipefail

# This script prints daemons names whos IAM role policies contain $pattern

pattern=$1

for daemon_name in $(ls ${DSS_HOME}/daemons); do
    if [[ ! -d "${DSS_HOME}/daemons/$daemon_name" ]]; then
        continue
	fi
	name=$daemon_name-${DSS_DEPLOYMENT_STAGE}
	aws lambda get-function --function-name $name > /dev/null 2>&1 || continue
	aws iam get-role-policy --role-name $name --policy-name $name | grep "${pattern}" > /dev/null && echo $name
done

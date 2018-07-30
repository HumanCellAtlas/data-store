#!/bin/bash

set -euo pipefail

if [[ $# != 1 ]]; then
	echo "This script fetches a secret value from AWS Secretsmanager, given a secret name."
    echo "Usage: $(basename $0) secret-name"
    echo "Example: $(basename $0) gcp-credentials.json"
    exit 1
fi

secret_name=${1}
aws secretsmanager get-secret-value --secret-id ${DSS_SECRETS_STORE}/${DSS_DEPLOYMENT_STAGE}/${secret_name} \
| jq -r .SecretString

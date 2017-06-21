#!/bin/bash

set -euo pipefail

if [[ $# != 3 ]]; then
    echo "Usage: $(basename $0) daemon-name stage lambda-input-file"
    exit 1
fi

lambda_name="$1-$2"
lambda_input_file=$3

lambda_payload="$(sed "s/testBucket/$DSS_S3_TEST_BUCKET/g" $lambda_input_file)"
lambda_output="$(aws lambda invoke --function-name $lambda_name --invocation-type RequestResponse --payload "$lambda_payload" --log-type Tail /dev/stdout | sed 's/^null//')"

echo "$lambda_output" | jq -r .LogResult | base64 --decode

[[ $(echo "$lambda_output" | jq -r .FunctionError) == null ]]

#!/bin/bash

set -euo pipefail

lambda_name="$1-$2"
lambda_input_file=$3

lambda_output="$(aws lambda invoke --function-name $lambda_name --invocation-type RequestResponse --payload "$(cat $lambda_input_file)" --log-type Tail /dev/stdout)"

echo "$lambda_output" | jq -r .LogResult | base64 --decode

[[ $(echo "$lambda_output" | jq -r .FunctionError) == null ]]

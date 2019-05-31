#!/bin/bash

set -euo pipefail

if [[ $# != 4 ]]; then
    echo "Usage: $(basename $0) daemon-name stage lambda-input-file bundle-file"
    exit 1
fi

lambda_name="$1-$2"
lambda_input_file=$3
bundle_file="$4"


BUNDLE_KEY="bundles/$(basename "${bundle_file}")"

if ! aws s3 ls s3://${DSS_S3_BUCKET}/"${BUNDLE_KEY}"; then
    aws s3 cp "${bundle_file}" s3://${DSS_S3_BUCKET}/"${BUNDLE_KEY}"
fi
BUNDLE_FILE_ETAG=$(aws s3api head-object --bucket ${DSS_S3_BUCKET} --key "${BUNDLE_KEY}" | jq -r '.ETag | fromjson')
BUNDLE_FILE_SIZE=$(cat "${bundle_file}" | wc -c)

# the wonky if-else is required because us-east-1 is represented as a null location constraint.  weird, eh?
DSS_S3_BUCKET_REGION=$(aws s3api get-bucket-location --bucket ${DSS_S3_BUCKET} | jq -r 'if (.LocationConstraint == null) then "us-east-1" else .LocationConstraint end')
envsubst_vars='$BUNDLE_KEY $BUNDLE_FILE_ETAG $BUNDLE_FILE_SIZE $DSS_S3_BUCKET $DSS_S3_BUCKET_REGION'
for varname in ${envsubst_vars}; do
    export ${varname##$}
done

raw_lambda_output="$(aws lambda invoke --function-name $lambda_name --invocation-type RequestResponse --payload "$(envsubst "${envsubst_vars}" < "${lambda_input_file}")" --log-type Tail /dev/stdout)"
lambda_output="$(echo $raw_lambda_output | jq -r '. | select(.LogResult)')"

# lambda output is occasionally malformed as appended JSON objects: {"wrong_obj": ... }{"LogResult": ...}
# This selects the object we wish to examine for error
echo "$lambda_output" | jq -r .LogResult | base64 --decode

[[ $(echo "$lambda_output" | jq -r .FunctionError) == null ]]

#!/bin/bash
# bash scripts/verify.sh OBJECT_TYPE CUTOFF_TIME
# OBJECT_TYPE is `files`, `bundles`, or `blobs`
# CUTOFF_TIME is lower limit as an iso8601 datetime
set -eu

if [[ "$#" -lt 2 ]]; then
    echo "./$0 OBJECT_TYPE CUTOFF_TIME"
    echo "OBJECT_TYPE is one of 'files', 'bundles', or 'blobs'"
    echo "CUTOFF_TIME determines which objects are selected for review and"
    echo "    is formatted as an iso8601 datetime. To verify all present"
    echo "    objects, provide a CUTOFF_TIME of zero (or something else"
    echo "    absurdly small)"
    exit 1
fi

OBJECT_TYPE="$1"
CUTOFF_TIME="'$2'"
SECRET_NAME="dcp/dss/${DSS_DEPLOYMENT_STAGE}/${OBJECT_TYPE}_last_verified"

# We want to get a list of all objects of prefix $TYPE that were created after
# $CUTOFF. To do that, we have to list all objects in $DSS_S3_BUCKET and sort
# them ourselves, locally.
aws s3api list-objects-v2 \
    --bucket "$DSS_S3_BUCKET" \
    --prefix "${OBJECT_TYPE}/" \
    --query "Contents[?LastModified > ${CUTOFF_TIME}]" > s3_${OBJECT_TYPE}_index
# The list of objects up for verification have at this point been written to
# s3_{files,bundles,blobs}_index.
jq '. | to_entries[] | .value.Key' s3_${OBJECT_TYPE}_index \
    | xargs -n 100 python scripts/dss-ops.py sync verify-sync \
        --source-replica aws --destination-replica gcp --keys
# If verification was completed successfully, let's update the last-verified
# time in SSM.
NEW_LAST_VERIFIED_TIME="$(jq '. | sort_by(.LastModified) | last.LastModified' s3_${OBJECT_TYPE}_index)"
python scripts/dss-ops.py secrets set --secret-name "$SECRET_NAME" --secret-value "$NEW_LAST_VERIFIED_TIME" > /dev/null

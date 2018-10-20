#!/bin/bash

set -euo pipefail

bucket=$1

echo "Are you sure you want to remove all objects from $bucket?"
select result in Yes No; do
    if [[ $result != Yes ]]; then exit 1; else break; fi
done

if [[ $bucket == s3://* ]]; then
    aws s3 rm $bucket --recursive --quiet
elif [[ $bucket == gs://* ]]; then
    gsutil -m rm -r $bucket
else
	exit 1
fi

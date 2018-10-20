#!/bin/bash

set -euo pipefail

replica=aws

for uuid in $(http GET "https://dss.integration.data.humancellatlas.org/v1/bundles/8633c0ef-463a-470b-994a-399c27e16883?replica=aws" | jq -r .bundle.files[].uuid); do
    echo $uuid;
    http -F GET "https://dss.integration.data.humancellatlas.org/v1/files/${uuid}?replica=${replica}"
done

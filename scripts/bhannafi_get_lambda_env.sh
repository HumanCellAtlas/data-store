#!/bin/bash

set -euo pipefail

env_json=$(aws ssm get-parameter --name /dcp/dss/dev/environment | jq .Parameter.Value | python -c "import sys, json; print(json.load(sys.stdin))")

for foo in $(echo $env_json | jq -r keys[]); do
    echo $foo $(echo $env_json | jq .$foo)
done

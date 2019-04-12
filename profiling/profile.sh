#!/bin/bash

set -euo pipefail

# Resolve the location of this file
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
export profile_dir="$(cd -P "$(dirname "$SOURCE")" && pwd)"

mem_size=${1}
date=$(date +%Y.%m.%d.%s)

git checkout -- ${DSS_HOME}/chalice && git clean -dfx ${DSS_HOME}/chalice
config_json="${DSS_HOME}/chalice/.chalice/config.json"
cat $config_json | jq ".lambda_memory_size=${mem_size}" | sponge "$config_json"
make -C ${DSS_HOME}/chalice

exec > "out_${date}_${mem_size}_bundles.txt"
exec 2>&1

${profile_dir}/profile_bundles.py --chunk-size 1000 --number-of-chunks 10

exec > "out_${date}_${mem_size}_collections.txt"
exec 2>&1

${profile_dir}/profile_collections.py --chunk-size 1000 --number-of-chunks 10

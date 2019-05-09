#!/usr/bin/env bash
# TODO make this description not suck
# This script is used to generate a requirements layer for chalice to use

set -euo pipefail

if [[ -z $DSS_DEPLOYMENT_STAGE ]]; then
    echo 'Please run "source environment" in the data-store repo root directory before running this command'
    exit 1
fi

# expand out the /op(ts folder to see what this looks like, rename and make clean.
# TODO this version of python 3.6 might change, need a stable way to get this
build_path="$DSS_HOME/dependencies/python/lib/python3.6/site-packages"
dependency_dir="$DSS_HOME/dependencies"
echo "downloading requirements to ${dependency_dir}"
pip -q --no-cache-dir install --target=${build_path} -r $DSS_HOME/requirements.txt # target looks like site-package
echo "compressing......"
cd ${dependency_dir}
zip -qq -r -o ${dependency_dir}.zip *
cd ..
echo "deleting ${dependency_dir}"
rm -rf ${dependency_dir}

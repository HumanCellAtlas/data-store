#!/usr/bin/env python
"""
This script runs a basic integration test of the DSS. It is invoked by Travis CI from a periodic cron job.
"""

import os, sys, argparse, platform, subprocess, glob, shutil, time
from tempfile import TemporaryDirectory

parser = argparse.ArgumentParser(description=__doc__)
args = parser.parse_args()

def GREEN(message=None):
    if message is None:
        return "\033[32m" if sys.stdout.isatty() else ""
    else:
        return GREEN() + message + ENDC()

def RED(message=None):
    if message is None:
        return "\033[31m" if sys.stdout.isatty() else ""
    else:
        return RED() + message + ENDC()

def ENDC():
    return "\033[0m" if sys.stdout.isatty() else ""

def run(command, **kwargs):
    if isinstance(command, str):
        kwargs["shell"] = True
    print(GREEN(command))
    try:
        subprocess.check_call(command, **kwargs)
    except subprocess.CalledProcessError as e:
        parser.exit(RED(f'{parser.prog}: Exit status {e.returncode} while running "{command}". Stopping.'))

run("git clone --depth 1 --recurse-submodules https://github.com/HumanCellAtlas/data-store-cli")
run("http --check-status https://${API_HOST}/v1/swagger.json > data-store-cli/swagger.json")
run("pip install -r data-store-cli/requirements.txt")
run("python -c 'import sys, hca.regenerate_api as r; r.generate_python_bindings(sys.argv[1])' swagger.json",
    cwd="data-store-cli")
run("find data-store-cli/hca -name '*.pyc' -delete")
run("pip install --upgrade .", cwd="data-store-cli")

bundle_dir = "data-bundle-examples/10X_v2/pbmc8k"
run(f"hca upload --replica aws --staging-bucket $DSS_S3_BUCKET_TEST --file-or-dir {bundle_dir} > upload.json")
run("hca download --replica aws $(jq -r .bundle_uuid upload.json)")
for i in range(10):
    try:
        run("http -v --check-status https://${API_HOST}/v1/bundles/$(jq -r .bundle_uuid upload.json)?replica=gcp")
        break
    except SystemExit:
        time.sleep(1)
else:
    parser.exit(RED("Failed to replicate bundle from AWS to GCP"))
run("hca download --replica gcp $(jq -r .bundle_uuid upload.json)")

run("hca post-search")
run('jq -n .query.match.foo=1 | http -v --check-status https://${API_HOST}/v1/search')

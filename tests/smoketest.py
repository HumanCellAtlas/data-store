#!/usr/bin/env python
"""
This script runs a basic integration test of the DSS. It is invoked by Travis CI from a periodic cron job.
"""

import os, sys, argparse, time, uuid, json
from subprocess import check_call, check_output, CalledProcessError
from tempfile import TemporaryDirectory

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.api.files import ASYNC_COPY_THRESHOLD

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

def run(command, runner=check_call, **kwargs):
    if isinstance(command, str):
        kwargs["shell"] = True
    print(GREEN(command))
    try:
        return runner(command, **kwargs)
    except CalledProcessError as e:
        parser.exit(RED(f'{parser.prog}: Exit status {e.returncode} while running "{command}". Stopping.'))

if not os.path.exists("data-store-cli"):
    run("git clone --depth 1 --recurse-submodules https://github.com/HumanCellAtlas/data-store-cli")

run("http --check-status https://${API_HOST}/v1/swagger.json > data-store-cli/swagger.json")
run("pip install -r data-store-cli/requirements.txt")
run("python -c 'import sys, hca.dss.regenerate_api as r; r.generate_python_bindings(sys.argv[1])' swagger.json",
    cwd="data-store-cli")
run("find data-store-cli/hca -name '*.pyc' -delete")
run("pip install --upgrade --no-deps .", cwd="data-store-cli")

sample_id = str(uuid.uuid4())
bundle_dir = "data-bundle-examples/10X_v2/pbmc8k"
with open(os.path.join(bundle_dir, "async_copied_file"), "wb") as fh:
    fh.write(os.urandom(ASYNC_COPY_THRESHOLD + 1))

run(f"cat {bundle_dir}/sample.json | jq .uuid=env.sample_id | sponge {bundle_dir}/sample.json",
    env=dict(os.environ, sample_id=sample_id))
run(f"hca dss upload --replica aws --staging-bucket $DSS_S3_BUCKET_TEST --file-or-dir {bundle_dir} > upload.json")
run("hca dss download --replica aws $(jq -r .bundle_uuid upload.json)")
for i in range(10):
    try:
        run("http -v --check-status https://${API_HOST}/v1/bundles/$(jq -r .bundle_uuid upload.json)?replica=gcp")
        break
    except SystemExit:
        time.sleep(1)
else:
    parser.exit(RED("Failed to replicate bundle from AWS to GCP"))
run("hca dss download --replica gcp $(jq -r .bundle_uuid upload.json)")

for replica in "aws", "gcp":
    run(f"hca dss post-search --es-query='{{}}' --output_format raw --replica {replica} > /dev/null")

search_route = "https://${API_HOST}/v1/search"
for replica in "aws", "gcp":
    run(f"jq -n '.es_query.query.match[env.k]=env.v' | http --check {search_route} replica==aws > res.json",
        env=dict(os.environ, k="files.sample_json.uuid", v=sample_id))
    with open("res.json") as fh2:
        res = json.load(fh2)
        print(json.dumps(res, indent=4))
        assert len(res["results"]) == 1

    res = run(f"hca dss put-subscriptions --callback-url https://example.com/ --es-query '{{}}' --replica {replica}",
              runner=check_output)
    sub_id = json.loads(res.decode())["uuid"]
    run(f"hca dss get-subscriptions --replica {replica}")
    run(f"hca dss delete-subscriptions --replica {replica} {sub_id}")

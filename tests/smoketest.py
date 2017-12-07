#!/usr/bin/env python
"""
This script runs a basic integration test of the DSS. It is invoked by Travis CI from a periodic cron job.
"""

import os, sys, argparse, time, uuid, json, shutil, tempfile
from subprocess import check_call, check_output, CalledProcessError

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.api.files import ASYNC_COPY_THRESHOLD

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--no-clean', dest='clean', action='store_false',
                    help="Don't remove the temporary working directory on exit.")
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


if os.path.exists("dcp-cli"):
    run("git pull --recurse-submodules", cwd="dcp-cli")
else:
    run("git clone --depth 1 --recurse-submodules https://github.com/HumanCellAtlas/dcp-cli")

workdir = tempfile.TemporaryDirectory(dir=os.getcwd(), prefix="smoketest-", suffix='.tmp')
try:
    venv = os.path.join(workdir.name, 'venv')
    run(f"virtualenv -p {sys.executable} {venv}")
    venv_bin = os.path.join(venv, 'bin', '')
    run(f"{venv_bin}pip install --upgrade .", cwd="dcp-cli")

    bundle_dir = os.path.join(workdir.name, "bundle")
    shutil.copytree("data-bundle-examples/10X_v2/pbmc8k", bundle_dir)
    sample_id = str(uuid.uuid4())
    with open(os.path.join(bundle_dir, "async_copied_file"), "wb") as fh:
        fh.write(os.urandom(ASYNC_COPY_THRESHOLD + 1))

    os.chdir(workdir.name)

    cli_config = {"DSSClient": {"swagger_url": os.environ["SWAGGER_URL"]}}
    cli_config_filename = f"{workdir.name}/cli_config.json"
    with open(cli_config_filename, "w") as fh2:
        fh2.write(json.dumps(cli_config))
    os.environ["HCA_CONFIG_FILE"] = f"{workdir.name}/cli_config.json"

    run(f"cat {bundle_dir}/sample.json | jq .uuid=env.sample_id | sponge {bundle_dir}/sample.json",
        env=dict(os.environ, sample_id=sample_id))
    run(f"{venv_bin}hca dss upload "
        "--replica aws "
        "--staging-bucket $DSS_S3_BUCKET_TEST "
        f"--src-dir {bundle_dir} > upload.json")
    run(f"{venv_bin}hca dss download --replica aws --bundle-uuid $(jq -r .bundle_uuid upload.json)")
    for i in range(10):
        try:
            run("http -v --check-status https://${API_HOST}/v1/bundles/$(jq -r .bundle_uuid upload.json)?replica=gcp")
            break
        except SystemExit:
            time.sleep(1)
    else:
        parser.exit(RED("Failed to replicate bundle from AWS to GCP"))
    run(f"{venv_bin}hca dss download --replica gcp --bundle-uuid $(jq -r .bundle_uuid upload.json)")

    for replica in "aws", "gcp":
        run(f"{venv_bin}hca dss post-search --es-query='{{}}' --replica {replica} > /dev/null")

    search_route = "https://${API_HOST}/v1/search"
    for replica in "aws", "gcp":
        run(f"jq -n '.es_query.query.match[env.k]=env.v' | http --check {search_route} replica==aws > res.json",
            env=dict(os.environ, k="files.sample_json.uuid", v=sample_id))
        with open("res.json") as fh2:
            res = json.load(fh2)
            print(json.dumps(res, indent=4))
            assert len(res["results"]) == 1

        res = run(f"{venv_bin}hca dss put-subscription "
                  "--callback-url https://example.com/ "
                  "--es-query '{}' "
                  f"--replica {replica}",
                  runner=check_output)
        sub_id = json.loads(res.decode())["uuid"]
        run(f"{venv_bin}hca dss get-subscriptions --replica {replica}")
        run(f"{venv_bin}hca dss delete-subscription --replica {replica} --uuid {sub_id}")
finally:
    if args.clean:
        workdir.cleanup()
    else:
        print(f"Leaving temporary working directory at {workdir}.", file=sys.stderr)
        # Disable workdir destructor
        workdir._finalizer.detach()  # type: ignore

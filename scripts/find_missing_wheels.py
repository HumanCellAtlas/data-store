#!/usr/bin/env python
"""
This script manages installation dependencies for AWS Lambda functions.

See http://chalice.readthedocs.io/en/latest/topics/packaging.html.
"""

import os, sys, argparse, platform, subprocess, glob, shutil
from tempfile import TemporaryDirectory

import chalice.deploy.packager
from chalice.deploy.packager import DependencyBuilder
from chalice.utils import OSUtils

def build_wheel(wheel_identifier, wheels_dir):
    print(f"Processing wheels for {wheel_identifier}")
    wd = os.path.join(wheels_dir, wheel_identifier)
    os.mkdir(wd)
    subprocess.check_output(["pip", "download", wheel_identifier], cwd=wd)
    if glob.glob(os.path.join(wd, "*.tar.gz")):
        subprocess.check_output("pip wheel *.tar.gz && rm -f *.tar.gz", shell=True, cwd=wd)
    # The sub-dependency wheels downloaded and built here do not obey the pins set by pip freeze in the overall
    # requirements.txt. We only keep the top level wheel. Since requirements.txt contains the whole flattened dependency
    # graph, each missing sub-dependency wheel is handled at top level in the overall scan.
    for wheel_filename in os.listdir(wd):
        if wheel_filename.find(wheel_identifier.replace("-", "_").replace("==", "-")) != 0:
            os.unlink(os.path.join(wd, wheel_filename))
    assert len(os.listdir(wd)) == 1, "Expected to find one wheel in {}, but found {}".format(wd, os.listdir(wd))

# See https://github.com/aws/chalice/issues/497 for discussion
chalice.deploy.packager.subprocess_python_base_environ = {"PATH": os.environ["PATH"]}

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("pip_reqs")
parser.add_argument("--build-wheels", nargs="*")
parser.add_argument("--wheels-dir", default="vendor.in")
args = parser.parse_args()

if args.build_wheels:
    if platform.system() != "Linux":
        parser.exit(f"{parser.prog}: Expected to run on a Linux system.")
    shutil.rmtree(args.wheels_dir)
    subprocess.call(["git", "checkout", "--", os.path.join(args.wheels_dir, ".keep")])
    for wheel_identifier in args.build_wheels:
        build_wheel(wheel_identifier, args.wheels_dir)
    print(f'Please run "git add {args.wheels_dir}" and commit the result.')
else:
    with TemporaryDirectory() as td:
        compat_wheels, missing_wheels = DependencyBuilder(OSUtils())._download_dependencies(td, args.pip_reqs)
        need_wheels = [w for w in missing_wheels if not os.path.exists(os.path.join(args.wheels_dir, w.identifier))]
        if need_wheels:
            msg = 'Missing wheels: {}. Please run "{}" in a Linux VM'
            parser.exit(msg.format(", ".join(wheel.identifier for wheel in need_wheels),
                                   " ".join(sys.argv + ["--build-wheels"] + [w.identifier for w in missing_wheels])))

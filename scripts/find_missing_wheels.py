#!/usr/bin/env python
"""
This script manages installation dependencies for AWS Lambda functions.

See http://chalice.readthedocs.io/en/latest/topics/packaging.html.
"""

import os, argparse, platform
from tempfile import TemporaryDirectory

from chalice.utils import OSUtils
from chalice.deploy.packager import DependencyBuilder

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("pip_reqs")
parser.add_argument("--build-wheels", action="store_true")
args = parser.parse_args()

with TemporaryDirectory() as td:
    compat_wheels, missing_wheels = DependencyBuilder(OSUtils())._download_dependencies(td, args.pip_reqs)
    for w in missing_wheels:
        print(w.identifier, w.filename)
'''
    need_wheels = [w for w in missing_wheels if not glob("vendor.in/{}.whl".format(w.identifier))]
    if need_wheels:
        need_wheel_ids = " ".join(wheel.identifier for wheel in missing_wheels)
        if args.build_wheels:
            if platform.system != "Linux":
                parser.exit("{}: Expected to run on a Linux system.".format(parser.prog))
            #pip download --dest vendor.in $$($(DSS_HOME)/scripts/find_missing_wheels.py requirements.txt)
            #pip wheel --wheel-dir vendor.in vendor.in/*.tar.gz
            #rm -f vendor.in/*.tar.gz
            print('Please run "git add vendor.in/*.whl" and commit the result.')
        else:
            parser.exit('Missing wheels: {}. Please run "FIXME --build-wheels" in a Linux VM'.format(need_wheel_ids))
'''

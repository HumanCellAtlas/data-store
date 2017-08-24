#!/usr/bin/env python
"""
This script manages installation dependencies for AWS Lambda functions.

See http://chalice.readthedocs.io/en/latest/topics/packaging.html.
"""

import os, argparse, platform, subprocess, glob
from tempfile import TemporaryDirectory

from chalice.utils import OSUtils
from chalice.deploy.packager import DependencyBuilder

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("pip_reqs")
parser.add_argument("--build-wheels", action="store_true")
parser.add_argument("--wheel-dir", default="vendor.in")
args = parser.parse_args()

with TemporaryDirectory() as td:
    compat_wheels, missing_wheels = DependencyBuilder(OSUtils())._download_dependencies(td, args.pip_reqs)
    need_wheels = [w for w in missing_wheels if not os.path.exists(os.path.join(args.wheel_dir, w.identifier))]
    if need_wheels:
        if args.build_wheels:
            if platform.system() != "Linux":
                parser.exit(f"{parser.prog}: Expected to run on a Linux system.")
            os.makedirs(args.wheel_dir)
            for wheel in need_wheels:
                wd = os.path.join(args.wheel_dir, wheel.identifier)
                os.mkdir(wd)
                subprocess.check_output(["pip", "download", wheel.identifier], cwd=wd)
                if glob.glob(os.path.join(wd, "*.tar.gz")):
                    subprocess.check_output("pip wheel *.tar.gz && rm -f *.tar.gz", shell=True, cwd=wd)
            print(f'Please run "git add {args.wheel_dir}" and commit the result.')
        else:
            w = " ".join(wheel.identifier for wheel in need_wheels)
            parser.exit(f'Missing wheels: {w}. Please run "{parser.prog} --build-wheels" in a Linux VM')

#!/usr/bin/env python
"""
This script manages installation dependencies for AWS Lambda functions.

See http://chalice.readthedocs.io/en/latest/topics/packaging.html.
"""

import os, argparse
from tempfile import TemporaryDirectory
from chalice.utils import OSUtils
from chalice.deploy.packager import DependencyBuilder

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("pip_requirements_file", dest="reqs")
args = parser.parse_args()

with TemporaryDirectory() as td:
    compatible_wheels, missing_wheels = DependencyBuilder(OSUtils())._download_dependencies(td, args.reqs)
    print(" ".join(wheel.identifier for wheel in missing_wheels))

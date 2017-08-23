#!/usr/bin/env python
"""
This script manages installation dependencies for AWS Lambda functions.

See http://chalice.readthedocs.io/en/latest/topics/packaging.html.
"""

from tempfile import TemporaryDirectory
from chalice.utils import OSUtils
from chalice.deploy.packager import DependencyBuilder

with TemporaryDirectory() as td:
    compatible_wheels, missing_wheels = DependencyBuilder(OSUtils())._download_dependencies(td, "requirements.txt")
    print(" ".join(wheel.identifier for wheel in missing_wheels))

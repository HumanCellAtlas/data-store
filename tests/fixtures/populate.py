#!/usr/bin/env python

import argparse
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.fixtures.populate_lib import populate

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Set up test fixtures in cloud storage buckets")
    parser.add_argument("--s3-bucket", type=str)
    parser.add_argument("--gs-bucket", type=str)

    args = parser.parse_args()

    populate(args.s3_bucket, args.gs_bucket)

    print("Fixtures populated. Run tests to ensure fixture integrity!")

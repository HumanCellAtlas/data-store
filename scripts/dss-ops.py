#!/usr/bin/env python
"""
Central entrypoint for DSS operational scripts
"""
import os
import sys
import logging

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
import dss.operations.storage
from dss.operations import dispatch

logging.basicConfig(stream=sys.stdout)
dss.Config.set_config(dss.BucketConfig.NORMAL)

if __name__ == "__main__":
    print(dispatch.job_id)
    dispatch(sys.argv[1:])

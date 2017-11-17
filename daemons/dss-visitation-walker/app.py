
import os
import sys
import json
import boto3
import domovoi
from time import time
from uuid import uuid4

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.stepfunctions.visitation.sfn_definitions import walker_sfn
from dss.stepfunctions.visitation import StatusCode, Walker
from dss.stepfunctions.visitation.utils import *
from dss import BucketConfig, Config


logger = dss.get_logger()
Config.set_config(BucketConfig.NORMAL)


app = domovoi.Domovoi()


@app.step_function_task(
    state_name = 'Initialize',
    state_machine_definition = walker_sfn
)
def initialize(event, context):

    walker = Walker(
        ** event,
        logger = logger
    )

    validate_bucket(
        walker.bucket
    )

    walker.code = StatusCode.RUNNING.name

    return walker.to_dict()


@app.step_function_task(
    state_name = 'Walk',
    state_machine_definition = walker_sfn
)
def walk(event, context):
    
    walker = Walker(
        ** event,
        logger = logger
    )

    walker.k_starts += 1

    handle, hca_handle, bucket = Config.get_cloud_specific_handles(
        walker.replica
    )

    start_time = time()
    elapsed_time = 0

    # TODO: stop work and return 'IN_PRORGRESS' before max return limit on handle.list is reached
    for item in boto3.resource("s3").Bucket(walker.bucket).objects.filter(
        Prefix = walker.prefix,
        Marker = walker.marker
    ):
        key = item.key

        try:
            # DO something
            # poke(handle, walker.bucket, key)
            print(key)

        except:
            logger.warning(f'walker_bee failed to process {key}')

        walker.marker = key
        walker.k_processed += 1

        if time() - start_time >= walker.timeout:

            return walker.to_dict(
                code = StatusCode.RUNNING.name
            )

    else:

        return walker.to_dict(
            code = StatusCode.SUCCEEDED.name
        )

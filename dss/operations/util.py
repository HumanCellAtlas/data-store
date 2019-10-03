import os
import sys
import traceback
import argparse
import logging
import typing
import time
from functools import wraps
from datetime import datetime, timedelta

from cloud_blobstore import BlobStore
from dcplib.aws.sqs import SQSMessenger

from dss.util.aws.clients import sts  # type: ignore
from dss.config import Config, Replica
from concurrent.futures import ThreadPoolExecutor, as_completed


logger = logging.getLogger(__name__)


_account_id = sts.get_caller_identity()['Account']
command_queue_url = "https://sqs.{}.amazonaws.com/{}/dss-operations-{}".format(
    os.environ['AWS_DEFAULT_REGION'],
    _account_id,
    os.environ['DSS_DEPLOYMENT_STAGE']
)


def map_bucket_results(func: typing.Callable, handle: BlobStore, bucket: str, base_pfx: str, parallelization=10):
    """
    Call `func` on an iterable of keys
    func is expected to be thread safe.
    """
    with ThreadPoolExecutor(max_workers=parallelization) as e:
        futures = list()
        for pfx in "0123456789abcdef":
            f = e.submit(func, handle.list(bucket, prefix=f"{base_pfx}{pfx}"))
            futures.append(f)
        for f in as_completed(futures):
            try:
                yield f.result()
            except Exception:
                logger.error(traceback.format_exc())

def map_bucket(*args, **kwargs):
    for _ in map_bucket_results(*args, **kwargs):
        pass

LOG_MONITOR_SLEEP_DURATION = 10

def monitor_logs(logs_client, job_id: str, start_time: datetime):
    start = new_start = int(1000 * (datetime.timestamp(datetime.utcnow())))
    log_group = f"/aws/lambda/dss-operations-{os.environ['DSS_DEPLOYMENT_STAGE']}"
    paginator = logs_client.get_paginator('filter_log_events')
    while True:
        for info in paginator.paginate(logGroupName=log_group, startTime=start, filterPattern=f'"{job_id}"'):
            for e in info['events']:
                print(e['message'])
                new_start = e['timestamp'] + 1
        if start == new_start:
            sys.stderr.write(f"no new CloudWatch log messages, sleeping {LOG_MONITOR_SLEEP_DURATION}s" + os.linesep)
            time.sleep(LOG_MONITOR_SLEEP_DURATION)
        else:
            start = new_start

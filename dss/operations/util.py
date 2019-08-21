import os
import traceback
import logging
import typing

from cloud_blobstore import BlobStore

from dss.util.aws.clients import sts  # type: ignore
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

def get_variable_prefix():
    """
    Use information from the environment to assemble
    the necessary prefix for accessing variables in
    the secrets manager or paramter store.
    """
    store_name = os.environ["DSS_PARAMETER_STORE"]
    stage_name = os.environ["DSS_DEPLOYMENT_STAGE"]
    store_prefix = f"{store_name}/{stage_name}"
    return store_prefix

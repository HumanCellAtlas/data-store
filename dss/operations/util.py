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

def set_cloud_env_var(
    environment: dict,
    set_fn,
    env_var: str,
    value,
    where: str = "",
) -> None:
    """
    Set a variable in a cloud environment by calling set_fn.
    (We pass the environment in directly, instead of passing
    a get_fn, because the get_fn may require input arguments.)

    Args:
        environment: A dictionary containing all environment variables.
        set_fn: A function handle used to set a new environment.
        env_var: The name of the environment variable to set.
        val: The value of the environment variable to set.
        where: A label for telling the user where the env var was set.
    """
    environment[env_var] = value
    set_fn(environment)
    print(f'Created variable "{env_var}" {where}')

def unset_cloud_env_var(
    environment: dict,
    set_fn,
    env_var: str,
    where: str = "",
) -> None:
    """
    Unset a variable in an environment by calling set_fn.

    Args:
        environment: A dictionary containing all environment variables.
        set_fn: A function handle used to set a new environment.
        env_var: The name of the environment variable to unset.
        where: A label for telling the user where the env var was unset.
    """
    try:
        del environment[env_var]
        set_fn(environment)
        print(f'Deleted parameter "{env_var}" {where}')
    except KeyError:
        print(f'Nothing to unset for parameter "{env_var}" {where}')

def get_cloud_variable_prefix():
    """
    Use information from the environment to assemble
    the necessary prefix for accessing variables in
    the secrets manager or paramter store.
    """
    store_name = os.environ["DSS_PARAMETER_STORE"]
    stage_name = os.environ["DSS_DEPLOYMENT_STAGE"]
    store_prefix = f"{store_name}/{stage_name}"
    return store_prefix

def fix_cloud_variable_prefix(secret_name):
    """
    This adds the variable store and stage prefix
    to the front of a variable name.
    """
    prefix = get_cloud_variable_prefix()
    if not secret_name.startswith(prefix):
        secret_name = prefix + secret_name
    return secret_name

class EmptyStdinException(Exception):
    def __init__(self):
        err_msg = f"Attempted to get a value from stdin, "
        err_msg += "but stdin was empty!"
        super.__init__(err_msg)

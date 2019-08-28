import os
import json
import traceback
import logging
import typing

from cloud_blobstore import BlobStore

from dss.util.aws.clients import sts  # type: ignore
from dss.util.aws.clients import es as es_client  # type: ignore
from dss.util.aws.clients import secretsmanager as sm_client  # type: ignore
from botocore.exceptions import ClientError

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
    env_set_fn,
    env_var: str,
    value,
    where: str = "",
) -> None:
    """
    Set a variable in a cloud environment by calling env_set_fn.
    (We pass the environment in directly, instead of passing
    a get_fn, because the get_fn may require input arguments.)

    Args:
        environment: A dictionary containing all environment variables.
        env_set_fn: A function handle used to set a new environment.
        env_var: The name of the environment variable to set.
        val: The value of the environment variable to set.
        where: A label for telling the user where the env var was set.
    """
    environment[env_var] = value
    env_set_fn(environment)
    print(f'Set variable "{env_var}" {where}')

def unset_cloud_env_var(
    environment: dict,
    env_set_fn,
    env_var: str,
    where: str = "",
) -> None:
    """
    Unset a variable in an environment by calling env_set_fn.

    Args:
        environment: A dictionary containing all environment variables.
        env_set_fn: A function handle used to set a new environment.
        env_var: The name of the environment variable to unset.
        where: A label for telling the user where the env var was unset.
    """
    try:
        del environment[env_var]
        env_set_fn(environment)
        print(f'Unset variable "{env_var}" {where}')
    except KeyError:
        print(f'Nothing to unset for variable "{env_var}" {where}')

def get_cloud_variable_prefix() -> str:
    """
    Use information from the environment to assemble
    the necessary prefix for accessing variables in
    the secrets manager or paramter store.
    """
    store_name = os.environ["DSS_PARAMETER_STORE"]
    stage_name = os.environ["DSS_DEPLOYMENT_STAGE"]
    store_prefix = f"{store_name}/{stage_name}"
    return store_prefix

def fix_cloud_variable_prefix(secret_name: str) -> str:
    """
    This adds the variable store and stage prefix
    to the front of a variable name.
    """
    prefix = get_cloud_variable_prefix()
    if not secret_name.startswith(prefix):
        secret_name = f"{prefix}/{secret_name}"
    return secret_name


def get_elasticsearch_endpoint() -> str:
    domain_name = os.environ['DSS_ES_DOMAIN']
    domain_info = es_client.describe_elasticsearch_domain(DomainName=domain_name)
    return domain_info['DomainStatus']['Endpoint']


def get_admin_emails() -> str:
    gcp_var = 'GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME'
    gcp_secret_id = fix_cloud_variable_prefix(gcp_var)

    admin_var = 'ADMIN_USER_EMAILS_SECRETS_NAME'
    admin_secret_id = fix_cloud_variable_prefix(admin_var)

    if secret_is_gettable(gcp_secret_id) and secret_is_gettable(admin_secret_id):
        resp = sm_client.get_secret_value(gcp_secret_id)
        gcp_service_account_email = json.loads(resp['SecretString'])['client_email']
        resp = sm_client.get_secret_value(admin_secret_id)
        email_list = [email for email in resp['SecretString'].split(',') if email.strip()]
        if gcp_service_account_email not in email_list:
            email_list.append(gcp_service_account_email)
        return ",".join(email_list)
    else:
        err = "Error adding Google service account email to admin emails list: "
        if secret_is_gettable(gcp_secret_id):
            err += f"Could not get secret {admin_var}"
            raise RuntimeError(err)
        elif secret_is_gettable(admin_secret_id):
            err += f"Could not get secret {gcp_secret_id}"
            raise RuntimeError(err)
        else:
            err += f"Unknown error occurred"
            raise RuntimeError(err)


# Secrets can be in three different states:
# - secret exists (gettable, settable)
# - secret exists but is marked for deletion (not gettable, not settable)
# - secret does not exist (not gettable, settable)

def secret_is_gettable(secret_name):
    """Secrets are gettable if they exist in the secrets manager"""
    try:
        sm_client.get_secret_value(SecretId=secret_name)
    except ClientError:
        return False
    else:
        return True

def secret_is_settable(secret_name):
    """Secrets are settable if they exist in the secrets manager or if they are not found"""
    try:
        sm_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if 'Error' in e.response:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return True
        return False
    else:
        return True


class EmptyStdinException(Exception):
    def __init__(self):
        err_msg = f"Attempted to get a value from stdin, "
        err_msg += "but stdin was empty!"
        super.__init__(err_msg)

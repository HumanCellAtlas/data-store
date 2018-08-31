"""
This is the module for bundle checkouts.
"""
import io
import json
import time
import typing

from dss import stepfunctions
from dss.config import Replica, Config
from dss.stepfunctions.checkout.constants import EventConstants, STATE_MACHINE_NAME_TEMPLATE
from dss.storage.bundles import get_bundle_manifest
from dss.storage.hcablobstore import BundleFileMetadata, BundleMetadata, compose_blob_key
from .common import CheckoutTokenKeys, get_execution_id
from .error import BundleNotFoundError, TokenError, CheckoutError


def start_bundle_checkout(
        replica: Replica,
        bundle_uuid: str,
        bundle_version: typing.Optional[str],
        dst_bucket: str,
        email_address: typing.Optional[str]=None,
        *,
        sts_bucket: typing.Optional[str]=None,
) -> str:
    """
    Starts a bundle checkout.

    :param bundle_uuid: The UUID of the bundle to check out.
    :param bundle_version: The version of the bundle to check out.  If this is not provided, the latest version of the
                           bundle is checked out.
    :param replica: The replica to execute the checkout in.
    :param dst_bucket: Check out to this bucket.
    :param email_address: If provided, send a message to this email address with the status of the checkout.
    :param sts_bucket: If provided, write the status of the checkout to this bucket.  If not provided, write the status
                       to the default checkout bucket for the replica.
    :return: The execution ID of the request.
    """

    bundle = get_bundle_manifest(bundle_uuid, replica, bundle_version)
    if bundle is None:
        raise BundleNotFoundError()
    execution_id = get_execution_id()
    if sts_bucket is None:
        sts_bucket = replica.checkout_bucket

    sfn_input = {
        EventConstants.DSS_BUCKET: replica.bucket,
        EventConstants.STATUS_BUCKET: sts_bucket,
        EventConstants.BUNDLE_UUID: bundle_uuid,
        EventConstants.BUNDLE_VERSION: bundle[BundleMetadata.VERSION],
        EventConstants.REPLICA: replica.name,
        EventConstants.EXECUTION_ID: execution_id
    }
    if dst_bucket is not None:
        sfn_input[EventConstants.DST_BUCKET] = dst_bucket

    if email_address is not None:
        sfn_input[EventConstants.EMAIL] = email_address

    mark_bundle_checkout_started(execution_id, replica, sts_bucket)

    stepfunctions.step_functions_invoke(STATE_MACHINE_NAME_TEMPLATE, execution_id, sfn_input)
    return execution_id


def verify_checkout(
        replica: Replica,
        bundle_uuid: str,
        bundle_version: typing.Optional[str],
        token: typing.Optional[str]
) -> typing.Tuple[str, bool]:
    decoded_token: dict
    if token is None:
        execution_id = start_bundle_checkout(replica, bundle_uuid, bundle_version, dst_bucket=replica.checkout_bucket)

        decoded_token = {
            CheckoutTokenKeys.EXECUTION_ID: execution_id,
            CheckoutTokenKeys.START_TIME: time.time(),
            CheckoutTokenKeys.ATTEMPTS: 0,
        }
    else:
        try:
            decoded_token = json.loads(token)
            execution_id = decoded_token[CheckoutTokenKeys.EXECUTION_ID]
            decoded_token[CheckoutTokenKeys.ATTEMPTS] += 1
        except (KeyError, ValueError) as ex:
            raise TokenError(ex)

    encoded_token = json.dumps(decoded_token)
    status = get_bundle_checkout_status(execution_id, replica, replica.checkout_bucket)
    if status['status'] == "SUCCEEDED":
        return encoded_token, True
    elif status['status'] == "RUNNING":
        return encoded_token, False

    raise CheckoutError(f"status: {status}")


def get_dst_bundle_prefix(bundle_id: str, bundle_version: str) -> str:
    return "bundles/{}.{}".format(bundle_id, bundle_version)


def get_manifest_files(replica: Replica, src_bucket: str, bundle_uuid: str, bundle_version: str):
    bundle_manifest = get_bundle_manifest(bundle_uuid, replica, bundle_version, bucket=src_bucket)
    files = bundle_manifest[BundleMetadata.FILES]
    dst_bundle_prefix = get_dst_bundle_prefix(bundle_uuid, bundle_manifest[BundleMetadata.VERSION])

    for file_metadata in files:
        dst_key = "{}/{}".format(dst_bundle_prefix, file_metadata.get(BundleFileMetadata.NAME))
        src_key = compose_blob_key(file_metadata)
        yield src_key, dst_key


_STATUS_KEY = "status"
_LOCATION_KEY = "location"
_CAUSE_KEY = "cause"


def _bundle_checkout_status_key(execution_id: str) -> str:
    return f"checkout/status/{execution_id}.json"


def mark_bundle_checkout_successful(
        execution_id: str,
        replica: Replica,
        sts_bucket: str,
        dst_bucket: str,
        dst_location: str,
):
    handle = Config.get_blobstore_handle(replica)
    data = {
        _STATUS_KEY: 'SUCCEEDED',
        _LOCATION_KEY: f"{replica.storage_schema}://{dst_bucket}/{dst_location}"
    }
    handle.upload_file_handle(
        sts_bucket,
        _bundle_checkout_status_key(execution_id),
        io.BytesIO(json.dumps(data).encode("utf-8")))


def mark_bundle_checkout_failed(execution_id: str, replica: Replica, sts_bucket: str, cause: str):
    handle = Config.get_blobstore_handle(replica)
    data = {_STATUS_KEY: "FAILED", _CAUSE_KEY: cause}
    handle.upload_file_handle(
        sts_bucket,
        _bundle_checkout_status_key(execution_id),
        io.BytesIO(json.dumps(data).encode("utf-8")))


def mark_bundle_checkout_started(execution_id: str, replica: Replica, sts_bucket: str):
    handle = Config.get_blobstore_handle(replica)
    data = {_STATUS_KEY: "RUNNING"}
    handle.upload_file_handle(
        sts_bucket,
        _bundle_checkout_status_key(execution_id),
        io.BytesIO(json.dumps(data).encode("utf-8")))


def get_bundle_checkout_status(execution_id: str, replica: Replica, sts_bucket: str):
    handle = Config.get_blobstore_handle(replica)
    return json.loads(handle.get(sts_bucket, _bundle_checkout_status_key(execution_id)))

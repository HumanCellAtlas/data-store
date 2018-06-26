import io
import json
import logging
import typing
import uuid
from enum import Enum, auto

from cloud_blobstore import BlobNotFoundError, BlobStoreUnknownError
from dss import stepfunctions
from dss.config import Config, Replica
from dss.stepfunctions import s3copyclient, gscopyclient
from dss.stepfunctions.checkout.constants import EventConstants
from dss.storage.bundles import get_bundle_manifest
from dss.storage.hcablobstore import BundleFileMetadata, BundleMetadata, compose_blob_key

log = logging.getLogger(__name__)


STATE_MACHINE_NAME_TEMPLATE = "dss-checkout-sfn-{stage}"


class ValidationEnum(Enum):
    NO_SRC_BUNDLE_FOUND = auto(),
    WRONG_DST_BUCKET = auto(),
    WRONG_PERMISSIONS_DST_BUCKET = auto(),
    WRONG_BUNDLE_KEY = auto(),
    PASSED = auto()


class BundleNotFoundError(Exception):
    """Raised when we attempt to check out a non-existent bundle."""
    pass


class CheckoutTokenKeys:
    """
    When we are executing a request that involves a checkout, the client will periodically check back in to see if the
    checkout is complete.  To avoid duplicating checkout requests, the client will check back using a token.  These are
    keys that make up the token.
    """
    EXECUTION_ID = "execution_id"
    """Execution ID of the step function managing the checkout."""

    START_TIME = "start_time"
    """Start time of the request."""

    ATTEMPTS = "attempts"
    """Number of times the client has attempted to check on the state of a checkout."""


def start_bundle_checkout(
        replica: Replica,
        bundle_uuid: str,
        bundle_version: typing.Optional[str],
        dst_bucket: typing.Optional[str] = None,
        email_address: typing.Optional[str] = None,
        *,
        sts_bucket: typing.Optional[str] = None,
) -> str:
    """
    Starts a bundle checkout.

    :param bundle_uuid: The UUID of the bundle to check out.
    :param bundle_version: The version of the bundle to check out.
    :param replica: The replica to execute the checkout in.
    :param dst_bucket: If provided, check out to this bucket.  If not provided, check out to the default checkout bucket
                       for the replica.
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
        EventConstants.EXECUTION_NAME: execution_id
    }
    if dst_bucket is not None:
        sfn_input[EventConstants.DST_BUCKET] = dst_bucket

    if email_address is not None:
        sfn_input[EventConstants.EMAIL] = email_address

    CheckoutStatus.mark_bundle_checkout_started(execution_id, replica, sts_bucket)

    stepfunctions.step_functions_invoke(STATE_MACHINE_NAME_TEMPLATE, execution_id, sfn_input)
    return execution_id


def start_file_checkout(replica: Replica, blob_key, dst_bucket: typing.Optional[str] = None) -> str:
    """
    Starts a file checkout.

    :param blob_key: The key of the blob that contains the file.
    :param replica: The replica to execute the checkout in.
    :param dst_bucket: If provided, check out to this bucket.  If not provided, check out to the default checkout bucket
                       for the replica.
    :return: The execution ID of the request.
    """
    if dst_bucket is None:
        dst_bucket = replica.checkout_bucket
    source_bucket = replica.bucket
    return parallel_copy(replica, source_bucket, blob_key, dst_bucket, get_dst_key(blob_key))


def parallel_copy(replica: Replica, source_bucket: str, source_key: str, destination_bucket: str, destination_key: str):
    log.debug(f"Copy file from bucket {source_bucket} with key {source_key} to "
              f"bucket {destination_bucket} destination file: {destination_key}")

    if replica == Replica.aws:
        state = s3copyclient.copy_sfn_event(
            source_bucket, source_key,
            destination_bucket, destination_key,
        )
        state_machine_name_template = "dss-s3-copy-sfn-{stage}"
    elif replica == Replica.gcp:
        state = gscopyclient.copy_sfn_event(
            source_bucket, source_key,
            destination_bucket, destination_key
        )
        state_machine_name_template = "dss-gs-copy-sfn-{stage}"
    else:
        raise ValueError("Unsupported replica")

    execution_name = get_execution_id()
    return stepfunctions.step_functions_invoke(state_machine_name_template, execution_name, state)


def get_dst_bundle_prefix(bundle_id: str, bundle_version: str) -> str:
    return "bundles/{}.{}".format(bundle_id, bundle_version)


def get_dst_key(blob_key: str):
    """
    Returns the destination key where a file checkout will be saved to.
    :param blob_key: The key for the file's data in the DSS bucket.
    :return: The key for the file's data in the checkout bucket.
    """
    return f"files/{blob_key}"


def get_manifest_files(replica: Replica, src_bucket: str, bundle_uuid: str, bundle_version: str):
    bundle_manifest = get_bundle_manifest(bundle_uuid, replica, bundle_version, bucket=src_bucket)
    files = bundle_manifest[BundleMetadata.FILES]
    dst_bundle_prefix = get_dst_bundle_prefix(bundle_uuid, bundle_manifest[BundleMetadata.VERSION])

    for file_metadata in files:
        dst_key = "{}/{}".format(dst_bundle_prefix, file_metadata.get(BundleFileMetadata.NAME))
        src_key = compose_blob_key(file_metadata)
        yield src_key, dst_key


def validate_file_dst(replica: Replica, dst_bucket: str, dst_key: str):
    try:
        Config.get_blobstore_handle(replica).get_user_metadata(dst_bucket, dst_key)
        return True
    except (BlobNotFoundError, BlobStoreUnknownError):
        return False


def pre_exec_validate(replica: Replica, dss_bucket: str, dst_bucket: str, bundle_uuid: str, bundle_version: str):
    validation_code, cause = validate_dst_bucket(replica, dst_bucket)
    if validation_code == ValidationEnum.PASSED:
        validation_code, cause = validate_bundle_exists(replica, dss_bucket, bundle_uuid, bundle_version)
    return validation_code, cause


def validate_dst_bucket(replica: Replica, dst_bucket: str) -> typing.Tuple[ValidationEnum, str]:
    if not Config.get_blobstore_handle(replica).check_bucket_exists(dst_bucket):
        return ValidationEnum.WRONG_DST_BUCKET, f"Bucket {dst_bucket} doesn't exist"
    if not touch_test_file(replica, dst_bucket):
        return ValidationEnum.WRONG_PERMISSIONS_DST_BUCKET, f"Insufficient permissions on bucket {dst_bucket}"

    return ValidationEnum.PASSED, None


def validate_bundle_exists(replica: Replica, dss_bucket: str, bundle_uuid: str, bundle_version: str):
    bundle_manifest = get_bundle_manifest(bundle_uuid, replica, bundle_version, bucket=dss_bucket)
    if bundle_manifest is None:
        return ValidationEnum.WRONG_BUNDLE_KEY, "Bundle with specified key does not exist"
    else:
        return ValidationEnum.PASSED, None


def get_execution_id() -> str:
    return str(uuid.uuid4())


def touch_test_file(replica: Replica, dst_bucket: str) -> bool:
    """
    Write a test file into the specified bucket.
    :param dst_bucket: the bucket to be checked.
    :param replica: the replica to execute the checkout in.
    :return: True if able to write, if not also returns error message as a cause
    """
    test_object = "touch.txt"
    handle = Config.get_blobstore_handle(replica)

    try:
        handle.upload_file_handle(
            dst_bucket,
            test_object,
            io.BytesIO(b""))
        Config.get_blobstore_handle(replica).delete(dst_bucket, test_object)
        return True
    except Exception:
        return False


class CheckoutStatus:
    STATUS_KEY = "status"
    LOCATION_KEY = "location"
    CAUSE_KEY = "cause"

    @classmethod
    def _bundle_checkout_status_key(cls, execution_id: str) -> str:
        return f"checkout/status/{execution_id}.json"

    @classmethod
    def mark_bundle_checkout_successful(
            cls,
            execution_id: str,
            replica: Replica,
            sts_bucket: str,
            dst_bucket: str,
            dst_location: str,
    ):
        handle = Config.get_blobstore_handle(replica)
        data = {
            CheckoutStatus.STATUS_KEY: 'SUCCEEDED',
            CheckoutStatus.LOCATION_KEY: f"{replica.storage_schema}://{dst_bucket}/{dst_location}"
        }
        handle.upload_file_handle(
            sts_bucket,
            cls._bundle_checkout_status_key(execution_id),
            io.BytesIO(json.dumps(data).encode("utf-8")))

    @classmethod
    def mark_bundle_checkout_failed(cls, execution_id: str, replica: Replica, sts_bucket: str, cause: str):
        handle = Config.get_blobstore_handle(replica)
        data = {CheckoutStatus.STATUS_KEY: "FAILED", CheckoutStatus.CAUSE_KEY: cause}
        handle.upload_file_handle(
            sts_bucket,
            cls._bundle_checkout_status_key(execution_id),
            io.BytesIO(json.dumps(data).encode("utf-8")))

    @classmethod
    def mark_bundle_checkout_started(cls, execution_id: str, replica: Replica, sts_bucket: str):
        handle = Config.get_blobstore_handle(replica)
        data = {CheckoutStatus.STATUS_KEY: "RUNNING"}
        handle.upload_file_handle(
            sts_bucket,
            cls._bundle_checkout_status_key(execution_id),
            io.BytesIO(json.dumps(data).encode("utf-8")))

    @classmethod
    def get_bundle_checkout_status(cls, execution_id: str, replica: Replica, sts_bucket: str):
        handle = Config.get_blobstore_handle(replica)
        return json.loads(handle.get(sts_bucket, cls._bundle_checkout_status_key(execution_id)))

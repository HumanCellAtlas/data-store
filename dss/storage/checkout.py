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
from dss.storage.bundles import get_bundle_manifest
from dss.storage.hcablobstore import compose_blob_key

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


def start_bundle_checkout(
        bundle_uuid: str,
        bundle_version: typing.Optional[str],
        replica: Replica,
        dst_bucket: typing.Optional[str]=None,
        email_address: typing.Optional[str]=None,
) -> str:
    """
    Starts a bundle checkout.

    :param bundle_uuid: The UUID of the bundle to check out.
    :param bundle_version: The version of the bundle to check out.
    :param replica: The replica to execute the checkout in.
    :param dst_bucket: If provided, check out to this bucket.  If not provided, check out to the default checkout bucket
                       for the replica.
    :param email_address: If provided, send a message to this email address with the status of the checkout.
    :return: The execution ID of the request.
    """

    bundle = get_bundle_manifest(bundle_uuid, replica, bundle_version)
    if bundle is None:
        raise BundleNotFoundError()
    execution_id = get_execution_id()

    sfn_input = {
        'dss_bucket': replica.bucket,
        'bundle': bundle_uuid,
        'version': bundle['version'],
        'replica': replica.name,
        'execution_name': execution_id
    }
    if dst_bucket is not None:
        sfn_input['bucket'] = dst_bucket

    if email_address is not None:
        sfn_input['email'] = email_address

    CheckoutStatus.mark_bundle_checkout_started(execution_id)

    stepfunctions.step_functions_invoke(STATE_MACHINE_NAME_TEMPLATE, execution_id, sfn_input)
    return execution_id


def start_file_checkout(
        blob_key,
        replica: Replica,
        dst_bucket: typing.Optional[str]=None,
) -> str:
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
    return parallel_copy(source_bucket, blob_key, dst_bucket, get_dst_key(blob_key), replica)


def parallel_copy(source_bucket: str, source_key: str, destination_bucket: str, destination_key: str, replica: Replica):
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


def get_manifest_files(src_bucket: str, bundle_id: str, version: str, replica: Replica):
    bundle_manifest = get_bundle_manifest(bundle_id, replica, version, bucket=src_bucket)
    files = bundle_manifest['files']
    dst_bundle_prefix = get_dst_bundle_prefix(bundle_id, bundle_manifest['version'])

    for file_metadata in files:
        dst_key = "{}/{}".format(dst_bundle_prefix, file_metadata.get('name'))
        src_key = compose_blob_key(file_metadata)
        yield src_key, dst_key


def validate_file_dst(dst_bucket: str, dst_key: str, replica: Replica):
    try:
        Config.get_blobstore_handle(replica).get_user_metadata(dst_bucket, dst_key)
        return True
    except (BlobNotFoundError, BlobStoreUnknownError):
        return False


def pre_exec_validate(dss_bucket: str, dst_bucket: str, replica: Replica, bundle_id: str, version: str):
    validation_code, cause = validate_dst_bucket(dst_bucket, replica)
    if validation_code == ValidationEnum.PASSED:
        validation_code, cause = validate_bundle_exists(replica, dss_bucket, bundle_id, version)
    return validation_code, cause


def validate_dst_bucket(dst_bucket: str, replica: Replica) -> typing.Tuple[ValidationEnum, str]:
    if not Config.get_blobstore_handle(replica).check_bucket_exists(dst_bucket):
        return ValidationEnum.WRONG_DST_BUCKET, f"Bucket {dst_bucket} doesn't exist"
    if not touch_test_file(dst_bucket, replica):
        return ValidationEnum.WRONG_PERMISSIONS_DST_BUCKET, f"Insufficient permissions on bucket {dst_bucket}"

    return ValidationEnum.PASSED, None


def validate_bundle_exists(replica: Replica, bucket: str, bundle_id: str, version: str):
    bundle_manifest = get_bundle_manifest(bundle_id, replica, version, bucket=bucket)
    if bundle_manifest is None:
        return ValidationEnum.WRONG_BUNDLE_KEY, "Bundle with specified key does not exist"
    else:
        return ValidationEnum.PASSED, None


def get_execution_id() -> str:
    return str(uuid.uuid4())


def touch_test_file(dst_bucket: str, replica: Replica) -> bool:
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
    @classmethod
    def _bundle_checkout_status_key(cls, execution_id: str) -> str:
        return f"checkout/status/{execution_id}.json"

    @classmethod
    def mark_bundle_checkout_successful(
            cls,
            execution_id: str,
            dst_replica: Replica,
            dst_bucket: str,
            dst_location: str
    ):
        handle = Config.get_blobstore_handle(Replica.aws)
        data = {"status": 'SUCCEEDED', "location": f"{dst_replica.storage_schema}://{dst_bucket}/{dst_location}"}
        handle.upload_file_handle(
            Replica.aws.checkout_bucket,
            cls._bundle_checkout_status_key(execution_id),
            io.BytesIO(json.dumps(data).encode("utf-8")))

    @classmethod
    def mark_bundle_checkout_failed(cls, execution_id: str, cause: str):
        handle = Config.get_blobstore_handle(Replica.aws)
        data = {"status": "FAILED", "cause": cause}
        handle.upload_file_handle(
            Replica.aws.checkout_bucket,
            cls._bundle_checkout_status_key(execution_id),
            io.BytesIO(json.dumps(data).encode("utf-8")))

    @classmethod
    def mark_bundle_checkout_started(cls, execution_id: str):
        handle = Config.get_blobstore_handle(Replica.aws)
        data = {"status": "RUNNING"}
        handle.upload_file_handle(
            Replica.aws.checkout_bucket,
            cls._bundle_checkout_status_key(execution_id),
            io.BytesIO(json.dumps(data).encode("utf-8")))

    @classmethod
    def get_bundle_checkout_status(cls, execution_id: str):
        handle = Config.get_blobstore_handle(Replica.aws)
        return json.loads(handle.get(Replica.aws.checkout_bucket, cls._bundle_checkout_status_key(execution_id)))

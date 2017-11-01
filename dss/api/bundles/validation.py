from enum import auto, Enum

from dss.api.bundles.helpers import get_destination_bucket
from dss.blobstore import BlobStore

class ValidationEnum(Enum):
    NO_SRC_BUNDLE_FOUND = auto(),
    PASSED = auto()

def validate_src(handle: BlobStore, bundle: str):
    return True

def validate_dst(handle: BlobStore, json_request_body: dict) -> ValidationEnum:
    if (not handle.check_bucket_exists(get_destination_bucket(json_request_body))):
        return ValidationEnum.NO_SRC_BUNDLE_FOUND
    return ValidationEnum.PASSED

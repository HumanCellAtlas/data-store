import os
import typing
from contextlib import contextmanager
from enum import Enum, unique

from .blobstore import BlobStore
from .blobstore.s3 import S3BlobStore
from .blobstore.gcs import GCSBlobStore
from .hcablobstore import HCABlobStore
from .hcablobstore.s3 import S3HCABlobStore
from .hcablobstore.gcs import GCSHCABlobStore


class BucketConfig(Enum):
    PROD = 0
    TEST = 1
    TEST_FIXTURE = 2

class Config(object):
    _S3_BUCKET = None  # type: str
    _GS_BUCKET = None  # type: str
    _CURRENT_CONFIG = BucketConfig.TEST  # type: BucketConfig

    @staticmethod
    def get_cloud_specific_handles(replica: str) -> typing.Tuple[
            BlobStore, HCABlobStore, str]:

        handle: BlobStore
        if replica == 'aws':
            handle = S3BlobStore()
            return (
                handle,
                S3HCABlobStore(handle),
                Config.get_s3_bucket()
            )
        elif replica == 'gcs':
            pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            credentials = os.path.join(pkg_root, "gcs-credentials.json")
            handle = GCSBlobStore(credentials)
            return (
                handle,
                GCSHCABlobStore(handle),
                Config.get_gs_bucket()
            )
        raise ValueError("I don't understand this replica!")

    @staticmethod
    def get_s3_bucket() -> str:
        # TODO: (ttung) right now, we use the bucket defined in
        # DSS_S3_TEST_BUCKET.  Eventually, the deployed version should use a
        # different bucket than the tests.
        #
        # Tests will continue to operate on the test bucket, however.
        if Config._S3_BUCKET is None:
            if Config._CURRENT_CONFIG == BucketConfig.TEST:
                envvar = "DSS_S3_TEST_BUCKET"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST_FIXTURE:
                envvar = "DSS_S3_TEST_SRC_DATA_BUCKET"

            if envvar not in os.environ:
                raise Exception(
                    "Please set the {} environment variable".format(envvar))
            Config._S3_BUCKET = os.environ[envvar]

        return Config._S3_BUCKET

    @staticmethod
    def get_gs_bucket() -> str:
        # TODO: (ttung) right now, we use the bucket defined in
        # DSS_GCS_TEST_BUCKET.  Eventually, the deployed version should use a
        # different bucket than the tests.
        #
        # Tests will continue to operate on the test bucket, however.
        if Config._CURRENT_CONFIG == BucketConfig.TEST:
            envvar = "DSS_GCS_TEST_BUCKET"
        elif Config._CURRENT_CONFIG == BucketConfig.TEST_FIXTURE:
            envvar = "DSS_GCS_TEST_SRC_DATA_BUCKET"

        if envvar not in os.environ:
            raise Exception(
                "Please set the {} environment variable".format(envvar))
        Config._GS_BUCKET = os.environ[envvar]

        return Config._GS_BUCKET

    @staticmethod
    def _clear_cached_config():
        # clear out the cached bucket settings.
        Config._S3_BUCKET = None
        Config._GS_BUCKET = None


@contextmanager
def override_bucket_config(temp_config: BucketConfig):
    original_config = Config._CURRENT_CONFIG
    Config._clear_cached_config()

    try:
        Config._CURRENT_CONFIG = temp_config
        yield
    finally:
        Config._CURRENT_CONFIG = original_config
        Config._clear_cached_config()

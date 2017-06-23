import os
import typing
from contextlib import contextmanager

from .blobstore import BlobStore
from .blobstore.s3 import S3BlobStore
from .blobstore.gcs import GCSBlobStore
from .hcablobstore import HCABlobStore
from .hcablobstore.s3 import S3HCABlobStore
from .hcablobstore.gcs import GCSHCABlobStore


class Config(object):
    _S3_BUCKET = None  # type: str
    _GS_BUCKET = None  # type: str

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
            if "DSS_S3_TEST_BUCKET" not in os.environ:
                raise Exception(
                    "Please set the DSS_S3_TEST_BUCKET environment variable")
            Config._S3_BUCKET = os.environ["DSS_S3_TEST_BUCKET"]

        return Config._S3_BUCKET

    @staticmethod
    def get_gs_bucket() -> str:
        # TODO: (ttung) right now, we use the bucket defined in
        # DSS_GCS_TEST_BUCKET.  Eventually, the deployed version should use a
        # different bucket than the tests.
        #
        # Tests will continue to operate on the test bucket, however.
        if Config._GS_BUCKET is None:
            if "DSS_GCS_TEST_BUCKET" not in os.environ:
                raise Exception(
                    "Please set the DSS_GCS_TEST_BUCKET environment variable")
            Config._GS_BUCKET = os.environ["DSS_GCS_TEST_BUCKET"]

        return Config._GS_BUCKET


@contextmanager
def override_s3_config(s3_bucket: str):
    original_s3_bucket = Config._S3_BUCKET
    try:
        Config._S3_BUCKET = s3_bucket
        yield
    finally:
        Config._S3_BUCKET = original_s3_bucket

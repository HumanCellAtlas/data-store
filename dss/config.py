import os
import typing

from .blobstore import BlobStore
from .blobstore.s3 import S3BlobStore
from .hcablobstore import HCABlobStore
from .hcablobstore.s3 import S3HCABlobStore


class Config(object):
    _S3_BUCKET = None  # type: str

    @staticmethod
    def get_cloud_specific_handles(replica: str) -> typing.Tuple[
            BlobStore, HCABlobStore, str]:
        if replica == 'AWS':
            handle = S3BlobStore()
            return (
                handle,
                S3HCABlobStore(handle),
                Config.get_s3_bucket()
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

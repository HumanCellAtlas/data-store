import os
import typing
from contextlib import contextmanager
from enum import Enum, auto

from .blobstore import BlobStore
from .blobstore.s3 import S3BlobStore
from .blobstore.gs import GSBlobStore
from .hcablobstore import HCABlobStore
from .hcablobstore.s3 import S3HCABlobStore
from .hcablobstore.gs import GSHCABlobStore


class BucketStage(Enum):
    ILLEGAL = auto()
    NORMAL = auto()
    TEST = auto()
    TEST_FIXTURE = auto()


class Config:
    _S3_BUCKET = None  # type: typing.Optional[str]
    _GS_BUCKET = None  # type: typing.Optional[str]
    _CURRENT_CONFIG = BucketStage.ILLEGAL  # type: BucketStage

    @staticmethod
    def set_config(config: BucketStage):
        Config._clear_cached_config()
        Config._CURRENT_CONFIG = config

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
        elif replica == 'gcp':
            credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            handle = GSBlobStore(credentials)
            return (
                handle,
                GSHCABlobStore(handle),
                Config.get_gs_bucket()
            )
        raise ValueError("I don't understand this replica!")

    @staticmethod
    def get_s3_bucket() -> str:
        if Config._S3_BUCKET is None:
            if Config._CURRENT_CONFIG == BucketStage.NORMAL:
                envvar = "DSS_S3_BUCKET"
            elif Config._CURRENT_CONFIG == BucketStage.TEST:
                    envvar = "DSS_S3_BUCKET_TEST"
            elif Config._CURRENT_CONFIG == BucketStage.TEST_FIXTURE:
                envvar = "DSS_S3_BUCKET_TEST_FIXTURES"
            elif Config._CURRENT_CONFIG == BucketStage.ILLEGAL:
                raise Exception("bucket config not set")

            if envvar not in os.environ:
                raise Exception(
                    "Please set the {} environment variable".format(envvar))
            Config._S3_BUCKET = os.environ[envvar]

        return Config._S3_BUCKET

    @staticmethod
    def get_gs_bucket() -> str:
        if Config._GS_BUCKET is None:
            if Config._CURRENT_CONFIG == BucketStage.NORMAL:
                envvar = "DSS_GS_BUCKET"
            elif Config._CURRENT_CONFIG == BucketStage.TEST:
                envvar = "DSS_GS_BUCKET_TEST"
            elif Config._CURRENT_CONFIG == BucketStage.TEST_FIXTURE:
                envvar = "DSS_GS_BUCKET_TEST_FIXTURES"
            elif Config._CURRENT_CONFIG == BucketStage.ILLEGAL:
                raise Exception("bucket config not set")

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
def override_bucket_config(temp_config: BucketStage):
    original_config = Config._CURRENT_CONFIG
    Config._clear_cached_config()

    try:
        Config._CURRENT_CONFIG = temp_config
        yield
    finally:
        Config._CURRENT_CONFIG = original_config
        Config._clear_cached_config()

import os
import typing
from contextlib import contextmanager
from enum import Enum, auto

from cloud_blobstore import BlobStore
from cloud_blobstore.s3 import S3BlobStore
from cloud_blobstore.gs import GSBlobStore

from .hcablobstore import HCABlobStore
from .hcablobstore.s3 import S3HCABlobStore
from .hcablobstore.gs import GSHCABlobStore


class DeploymentStage(Enum):
    ILLEGAL = auto()
    NORMAL = auto()
    TEST = auto()
    TEST_FIXTURE = auto()

class ESIndexType(Enum):
    docs = auto()
    subscriptions = auto()

class ESDocType(Enum):
    doc = auto()  # Metadata docs in the dss-docs index
    query = auto()  # Percolate queries (for event subscriptions) in the dss-docs index
    subscription = auto()  # Event subscriptions in the ds-subscriptions index

class Replica(Enum):
    aws = auto()
    gcp = auto()

class IndexSuffix:
    '''Creates a test specific index when in test config.'''
    __suf__ = ''  # index suffix to use

    @property
    def name(self):
        return self.__suf__

    @name.setter
    def name(self, value: str):
        self.__suf__ = value

    @classmethod
    def reset(cls):
        cls.name = ''

class Config:
    _S3_BUCKET = None  # type: typing.Optional[str]
    _GS_BUCKET = None  # type: typing.Optional[str]
    _ALLOWED_EMAILS = None  # type: typing.Optional[str]
    _CURRENT_CONFIG = DeploymentStage.ILLEGAL  # type: DeploymentStage

    @staticmethod
    def set_config(config: DeploymentStage):
        Config._clear_cached_bucket_config()
        Config._clear_cached_email_config()
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
    def get_storage_schema(replica: str) -> str:
        if replica == 'aws':
            return "s3"
        elif replica == 'gcp':
            return "gs"
        raise ValueError("I don't understand this replica!")

    @staticmethod
    def get_s3_bucket() -> str:
        if Config._S3_BUCKET is None:
            if Config._CURRENT_CONFIG == DeploymentStage.NORMAL:
                envvar = "DSS_S3_BUCKET"
            elif Config._CURRENT_CONFIG == DeploymentStage.TEST:
                envvar = "DSS_S3_BUCKET_TEST"
            elif Config._CURRENT_CONFIG == DeploymentStage.TEST_FIXTURE:
                envvar = "DSS_S3_BUCKET_TEST_FIXTURES"
            elif Config._CURRENT_CONFIG == DeploymentStage.ILLEGAL:
                raise Exception("bucket config not set")

            if envvar not in os.environ:
                raise Exception(
                    "Please set the {} environment variable".format(envvar))
            Config._S3_BUCKET = os.environ[envvar]

        return Config._S3_BUCKET

    @staticmethod
    def get_gs_bucket() -> str:
        if Config._GS_BUCKET is None:
            if Config._CURRENT_CONFIG == DeploymentStage.NORMAL:
                envvar = "DSS_GS_BUCKET"
            elif Config._CURRENT_CONFIG == DeploymentStage.TEST:
                envvar = "DSS_GS_BUCKET_TEST"
            elif Config._CURRENT_CONFIG == DeploymentStage.TEST_FIXTURE:
                envvar = "DSS_GS_BUCKET_TEST_FIXTURES"
            elif Config._CURRENT_CONFIG == DeploymentStage.ILLEGAL:
                raise Exception("bucket config not set")

            if envvar not in os.environ:
                raise Exception(
                    "Please set the {} environment variable".format(envvar))
            Config._GS_BUCKET = os.environ[envvar]

        return Config._GS_BUCKET

    @staticmethod
    def get_allowed_email_domains() -> str:
        if Config._ALLOWED_EMAILS is None:
            if Config._CURRENT_CONFIG == DeploymentStage.NORMAL:
                envvar = "DSS_SUBSCRIPTION_AUTHORIZED_DOMAINS"
            elif Config._CURRENT_CONFIG == DeploymentStage.TEST:
                envvar = "DSS_SUBSCRIPTION_AUTHORIZED_DOMAINS_TEST"
            elif Config._CURRENT_CONFIG == DeploymentStage.TEST_FIXTURE:
                envvar = "DSS_SUBSCRIPTION_AUTHORIZED_DOMAINS_TEST"
            elif Config._CURRENT_CONFIG == DeploymentStage.ILLEGAL:
                raise Exception("authorized domains config not set")

            if envvar not in os.environ:
                raise Exception(
                    f"Please set the {envvar} environment variable")
            Config._ALLOWED_EMAILS = os.environ[envvar]

        return Config._ALLOWED_EMAILS

    @staticmethod
    def get_es_index_name(index_type: ESIndexType, replica: Replica) -> str:
        deployment_stage = os.environ["DSS_DEPLOYMENT_STAGE"]
        index = f"dss-{index_type.name}-{replica.name}-{deployment_stage}"
        if Config._CURRENT_CONFIG == DeploymentStage.TEST:
            index = f"{index}-{IndexSuffix.name}"
        return index

    @staticmethod
    def _clear_cached_bucket_config():
        # clear out the cached bucket settings.
        Config._S3_BUCKET = None
        Config._GS_BUCKET = None

    @staticmethod
    def _clear_cached_email_config():
        # clear out the cached email settings.
        Config._ALLOWED_EMAILS = None


@contextmanager
def override_bucket_config(temp_config: DeploymentStage):
    original_config = Config._CURRENT_CONFIG
    Config._clear_cached_bucket_config()

    try:
        Config._CURRENT_CONFIG = temp_config
        yield
    finally:
        Config._CURRENT_CONFIG = original_config
        Config._clear_cached_bucket_config()

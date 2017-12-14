import os
import typing
from contextlib import contextmanager
from enum import Enum, EnumMeta, auto

from cloud_blobstore import BlobStore
from cloud_blobstore.s3 import S3BlobStore
from cloud_blobstore.gs import GSBlobStore

from .hcablobstore import HCABlobStore
from .hcablobstore.s3 import S3HCABlobStore
from .hcablobstore.gs import GSHCABlobStore


class DeploymentStageMeta(EnumMeta):
    _MAGIC_PREFIX = "IS_"

    def __getattr__(cls, item: str):
        if item.startswith(DeploymentStageMeta._MAGIC_PREFIX):
            trailer = item[len(DeploymentStageMeta._MAGIC_PREFIX):]
            attr = getattr(DeploymentStage, trailer, None)
            if isinstance(attr, DeploymentStage):
                return lambda: os.environ["DSS_DEPLOYMENT_STAGE"] == attr.value
        raise AttributeError(item)


class DeploymentStage(Enum, metaclass=DeploymentStageMeta):
    """
    Represents the current deployment stage.  Through the `DeploymentStageMeta` metaclass, we provide the magic methods
    IS_{STAGE}, which return True if the current deployment stage matches `STAGE`.

    e.g., IS_PROD() will return True iff the current deployment is a production deployment.
    """
    PROD = "prod"
    STAGING = "staging"
    DEV = "dev"


class BucketConfig(Enum):
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
    name = ''  # type: typing.Optional[str]

    @staticmethod
    def reset():
        IndexSuffix.name = ''


class Config:
    _S3_BUCKET = None  # type: typing.Optional[str]
    _GS_BUCKET = None  # type: typing.Optional[str]
    _S3_CHECKOUT_BUCKET = None  # type: typing.Optional[str]

    _ALLOWED_EMAILS = None  # type: typing.Optional[str]
    _CURRENT_CONFIG = BucketConfig.ILLEGAL  # type: BucketConfig
    _NOTIFICATION_SENDER_EMAIL = None  # type: typing.Optional[str]

    @staticmethod
    def set_config(config: BucketConfig):
        Config._clear_cached_bucket_config()
        Config._clear_cached_email_config()
        Config._CURRENT_CONFIG = config

    @staticmethod
    def get_cloud_specific_handles(replica: Replica) -> typing.Tuple[BlobStore, HCABlobStore, str]:

        if type(replica) != Replica:
            raise ValueError(f"{replica} is not a Replica!")

        handle: BlobStore
        if replica == Replica.aws:
            handle = S3BlobStore()
            return (
                handle,
                S3HCABlobStore(handle),
                Config.get_s3_bucket()
            )
        elif replica == Replica.gcp:
            credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            handle = GSBlobStore(credentials)
            return (
                handle,
                GSHCABlobStore(handle),
                Config.get_gs_bucket()
            )
        raise NotImplementedError(f"Replica `{replica.name}` is not implemented!")

    @staticmethod
    def get_storage_schema(replica: Replica) -> str:

        if type(replica) != Replica:
            raise ValueError(f"{replica} is not a Replica!")

        if replica == Replica.aws:
            return "s3"
        elif replica == Replica.gcp:
            return "gs"
        raise NotImplementedError(f"Replica `{replica.name}` is not implemented!")

    @staticmethod
    def get_s3_bucket() -> str:
        if Config._S3_BUCKET is None:
            if Config._CURRENT_CONFIG == BucketConfig.NORMAL:
                envvar = "DSS_S3_BUCKET"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST:
                envvar = "DSS_S3_BUCKET_TEST"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST_FIXTURE:
                envvar = "DSS_S3_BUCKET_TEST_FIXTURES"
            elif Config._CURRENT_CONFIG == BucketConfig.ILLEGAL:
                raise Exception("bucket config not set")

            if envvar not in os.environ:
                raise Exception(
                    "Please set the {} environment variable".format(envvar))
            Config._S3_BUCKET = os.environ[envvar]

        return Config._S3_BUCKET

    @staticmethod
    def get_s3_checkout_bucket() -> str:
        if Config._S3_CHECKOUT_BUCKET is None:
            if Config._CURRENT_CONFIG == BucketConfig.NORMAL:
                envvar = "DSS_S3_CHECKOUT_BUCKET"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST:
                envvar = "DSS_S3_CHECKOUT_BUCKET_TEST"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST_FIXTURE:
                envvar = "DSS_S3_CHECKOUT_BUCKET_TEST_FIXTURES"
            elif Config._CURRENT_CONFIG == BucketConfig.ILLEGAL:
                raise Exception("bucket config not set")

            if envvar not in os.environ:
                raise Exception(
                    "Please set the {} environment variable".format(envvar))
            Config._S3_CHECKOUT_BUCKET = os.environ[envvar]

        return Config._S3_CHECKOUT_BUCKET

    @staticmethod
    def get_gs_bucket() -> str:
        if Config._GS_BUCKET is None:
            if Config._CURRENT_CONFIG == BucketConfig.NORMAL:
                envvar = "DSS_GS_BUCKET"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST:
                envvar = "DSS_GS_BUCKET_TEST"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST_FIXTURE:
                envvar = "DSS_GS_BUCKET_TEST_FIXTURES"
            elif Config._CURRENT_CONFIG == BucketConfig.ILLEGAL:
                raise Exception("bucket config not set")

            if envvar not in os.environ:
                raise Exception(
                    "Please set the {} environment variable".format(envvar))
            Config._GS_BUCKET = os.environ[envvar]

        return Config._GS_BUCKET

    @staticmethod
    def get_allowed_email_domains() -> str:
        if Config._ALLOWED_EMAILS is None:
            if Config._CURRENT_CONFIG == BucketConfig.NORMAL:
                envvar = "DSS_SUBSCRIPTION_AUTHORIZED_DOMAINS"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST:
                envvar = "DSS_SUBSCRIPTION_AUTHORIZED_DOMAINS_TEST"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST_FIXTURE:
                envvar = "DSS_SUBSCRIPTION_AUTHORIZED_DOMAINS_TEST"
            elif Config._CURRENT_CONFIG == BucketConfig.ILLEGAL:
                raise Exception("authorized domains config not set")

            if envvar not in os.environ:
                raise Exception(
                    f"Please set the {envvar} environment variable")
            Config._ALLOWED_EMAILS = os.environ[envvar]

        return Config._ALLOWED_EMAILS

    @staticmethod
    def get_es_index_name(index_type: ESIndexType,
                          replica: Replica,
                          shape_descriptor: typing.Optional[str] = None
                          ) -> str:
        """
        Returns the fully qualified name of an Elasticsearch index of documents for a given
        replica of a given type and shape.
        """
        deployment_stage = os.environ["DSS_DEPLOYMENT_STAGE"]
        index = f"dss-{deployment_stage}-{replica.name}-{index_type.name}"
        if shape_descriptor:
            index = f"{index}-{shape_descriptor}"
        if Config._CURRENT_CONFIG == BucketConfig.TEST:
            index = f"{index}.{IndexSuffix.name}"
        return index

    @staticmethod
    def get_es_alias_name(index_type: ESIndexType, replica: Replica) -> str:
        """Returns the alias for indexes"""
        deployment_stage = os.environ["DSS_DEPLOYMENT_STAGE"]
        index = f"dss-{deployment_stage}-{replica.name}-{index_type.name}-alias"
        if Config._CURRENT_CONFIG == BucketConfig.TEST:
            index = f"{index}.{IndexSuffix.name}"
        return index

    @staticmethod
    def _clear_cached_bucket_config():
        # clear out the cached bucket settings.
        Config._S3_BUCKET = None
        Config._GS_BUCKET = None
        Config._S3_CHECKOUT_BUCKET = None

    @staticmethod
    def _clear_cached_email_config():
        # clear out the cached email settings.
        Config._ALLOWED_EMAILS = None
        Config._NOTIFICATION_SENDER_EMAIL = None

    @staticmethod
    def get_notification_email() -> str:
        envvar = "DSS_NOTIFICATION_SENDER"
        if envvar not in os.environ:
            raise Exception(
                "Please set the {} environment variable".format(envvar))
        Config._NOTIFICATION_SENDER_EMAIL = os.environ[envvar]

        return Config._NOTIFICATION_SENDER_EMAIL


@contextmanager
def override_bucket_config(temp_config: BucketConfig):
    original_config = Config._CURRENT_CONFIG
    Config._clear_cached_bucket_config()

    try:
        Config._CURRENT_CONFIG = temp_config
        yield
    finally:
        Config._CURRENT_CONFIG = original_config
        Config._clear_cached_bucket_config()

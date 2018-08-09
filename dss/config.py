from collections import deque
import functools
import os
import typing
from contextlib import contextmanager
from enum import Enum, EnumMeta, auto

import boto3
import botocore.config
from cloud_blobstore import BlobStore
from cloud_blobstore.s3 import S3BlobStore
from cloud_blobstore.gs import GSBlobStore
from google.cloud.storage import Client
from google.oauth2 import service_account
import google.resumable_media.common

from dss.storage.hcablobstore import HCABlobStore
from dss.storage.hcablobstore.s3 import S3HCABlobStore
from dss.storage.hcablobstore.gs import GSHCABlobStore


class DeploymentStageMeta(EnumMeta):
    _MAGIC_PREFIX = "IS_"

    def __getattr__(cls, item: str):
        if item.startswith(DeploymentStageMeta._MAGIC_PREFIX):
            trailer = item[len(DeploymentStageMeta._MAGIC_PREFIX):]
            attr = getattr(DeploymentStage, trailer, None)
            if isinstance(attr, DeploymentStage):
                return lambda: Config.deployment_stage() == attr.value
        raise AttributeError(item)


class DeploymentStage(Enum, metaclass=DeploymentStageMeta):
    """
    Represents the current deployment stage.  Through the `DeploymentStageMeta` metaclass, we provide the magic methods
    IS_{STAGE}, which return True if the current deployment stage matches `STAGE`.

    e.g., IS_PROD() will return True if the current deployment is a production deployment.
    """
    PROD = "prod"
    STAGING = "staging"
    INTEGRATION = "integration"
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


class IndexSuffix:
    """
    Manage growing and shrinking suffixes to Elasticsearch index names
    """
    def __init__(self) -> None:
        super().__init__()
        self._stack: typing.Deque[str] = deque()

    @property
    def value(self):
        """
        Return the current suffix
        """
        return self._stack[-1] if self._stack else ''

    def prepend(self, prefix: str):
        """
        Save the current suffix for later restore() and extend it by prepending the given prefix
        """
        assert prefix.islower(), 'Suffix prefixes must be lower case.'
        self._stack.append('-' + prefix + self.value)

    def restore(self):
        """
        Restore the previous suffix
        """
        self._stack.pop()


class Config:
    _S3_BUCKET: typing.Optional[str] = None
    _GS_BUCKET: typing.Optional[str] = None
    _S3_CHECKOUT_BUCKET: typing.Optional[str] = None
    _GS_CHECKOUT_BUCKET: typing.Optional[str] = None

    BLOBSTORE_CONNECT_TIMEOUT: float = None
    BLOBSTORE_READ_TIMEOUT: float = None
    BLOBSTORE_RETRIES: int = None

    _ALLOWED_EMAILS: typing.Optional[str] = None
    _EMAIL_WHITELIST: typing.Optional[str] = None
    _CURRENT_CONFIG: BucketConfig = BucketConfig.ILLEGAL
    _NOTIFICATION_SENDER_EMAIL: typing.Optional[str] = None

    test_index_suffix = IndexSuffix()

    @staticmethod
    def set_config(config: BucketConfig):
        Config._clear_cached_bucket_config()
        Config._clear_cached_email_config()
        Config._CURRENT_CONFIG = config

    @staticmethod
    @functools.lru_cache()
    def get_native_handle(replica: "Replica") -> typing.Any:
        if replica == Replica.aws:
            return Config._get_native_aws_handle()
        elif replica == Replica.gcp:
            return Config._get_native_gcp_handle()
        raise NotImplementedError(f"Replica `{replica.name}` is not implemented!")

    @staticmethod
    def _get_native_aws_handle() -> typing.Any:
        boto_config = botocore.config.Config()
        if Config.BLOBSTORE_CONNECT_TIMEOUT is not None:
            boto_config.connect_timeout = Config.BLOBSTORE_CONNECT_TIMEOUT
        if Config.BLOBSTORE_READ_TIMEOUT is not None:
            boto_config.read_timeout = Config.BLOBSTORE_READ_TIMEOUT
        if Config.BLOBSTORE_RETRIES is not None:
            boto_config.retries = {'max_attempts': Config.BLOBSTORE_RETRIES}
        return boto3.client("s3", config=boto_config)

    @staticmethod
    def _get_native_gcp_handle() -> typing.Any:
        if Config.BLOBSTORE_CONNECT_TIMEOUT is None and Config.BLOBSTORE_READ_TIMEOUT is None:
            client = Client.from_service_account_json(
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
            )
        else:
            # GCP has no direct interface to configure retries and timeouts. However, it makes use of Python's
            # stdlib `requests` package, which has straightforward timeout usage.
            class SessionWithTimeouts(google.auth.transport.requests.AuthorizedSession):
                def request(self, *args, **kwargs):
                    kwargs['timeout'] = (Config.BLOBSTORE_CONNECT_TIMEOUT, Config.BLOBSTORE_READ_TIMEOUT)
                    return super().request(*args, **kwargs)

            credentials = service_account.Credentials.from_service_account_file(
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
                scopes=Client.SCOPE
            )

            # _http is a "private" parameter, and we may need to re-visit GCP timeout retry
            # strategies in the future.
            client = Client(
                _http=SessionWithTimeouts(credentials),
                credentials=credentials
            )

        if Config.BLOBSTORE_RETRIES is not None:
            from requests.adapters import HTTPAdapter
            from requests.packages.urllib3.util.retry import Retry
            retry = Retry(total=Config.BLOBSTORE_RETRIES,
                          backoff_factor=0.3,
                          status_forcelist=(500, 502, 504)
                          )
            adapter = HTTPAdapter(max_retries=retry)
            # _http is a "private" parameter, and we may need to re-visit GCP timeout retry
            # strategies in the future.
            client._http.mount('https://', adapter)
            client._http.mount('http://', adapter)
        return client

    @staticmethod
    @functools.lru_cache()
    def get_blobstore_handle(replica: "Replica") -> BlobStore:
        return replica.blobstore_class(Config.get_native_handle(replica))

    @staticmethod
    @functools.lru_cache()
    def get_hcablobstore_handle(replica: "Replica") -> HCABlobStore:
        return replica.hcablobstore_class(Config.get_blobstore_handle(replica))

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
                envvar = "DSS_S3_CHECKOUT_BUCKET_TEST"
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
    def get_gs_checkout_bucket() -> str:
        if Config._GS_CHECKOUT_BUCKET is None:
            if Config._CURRENT_CONFIG == BucketConfig.NORMAL:
                envvar = "DSS_GS_CHECKOUT_BUCKET"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST:
                envvar = "DSS_GS_CHECKOUT_BUCKET_TEST"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST_FIXTURE:
                envvar = "DSS_GS_CHECKOUT_BUCKET_TEST"
            elif Config._CURRENT_CONFIG == BucketConfig.ILLEGAL:
                raise Exception("bucket config not set")

            if envvar not in os.environ:
                raise Exception(
                    "Please set the {} environment variable".format(envvar))
            Config._GS_CHECKOUT_BUCKET = os.environ[envvar]

        return Config._GS_CHECKOUT_BUCKET

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
    def get_email_whitelist_name() -> str:
        if Config._EMAIL_WHITELIST is None:
            if Config._CURRENT_CONFIG == BucketConfig.NORMAL:
                envvar = "EMAIL_WHITELIST_NAME"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST:
                envvar = "EMAIL_WHITELIST_NAME"
            elif Config._CURRENT_CONFIG == BucketConfig.TEST_FIXTURE:
                envvar = "EMAIL_WHITELIST_NAME"
            elif Config._CURRENT_CONFIG == BucketConfig.ILLEGAL:
                raise Exception("email whitelist is not set")

            if envvar not in os.environ:
                raise Exception(
                    f"Please set the {envvar} environment variable")
            Config._EMAIL_WHITELIST = os.environ[envvar]

        return Config._EMAIL_WHITELIST

    @staticmethod
    def get_es_index_name(index_type: ESIndexType,
                          replica: "Replica",
                          shape_descriptor: typing.Optional[str] = None
                          ) -> str:
        """
        Returns the fully qualified name of an Elasticsearch index containing documents of the given type in the given
        replica.
        """
        return Config._get_es_index_name(index_type, replica, shape_descriptor)

    @staticmethod
    def get_es_alias_name(index_type: ESIndexType, replica: "Replica") -> str:
        """
        Returns the alias for the set of Elasticsearch indexes containing documents of the given type in the given
        replica.
        """
        return Config._get_es_index_name(index_type, replica, 'alias')

    @classmethod
    def _get_es_index_name(cls,
                           index_type: ESIndexType,
                           replica: 'Replica',
                           suffix: typing.Optional[str] = None) -> str:
        index = f"dss-{cls.deployment_stage()}-{replica.name}-{index_type.name}"
        if suffix:
            index = f"{index}-{suffix}"
        index += cls.test_index_suffix.value
        return index

    @classmethod
    def deployment_stage(cls) -> str:
        return os.environ['DSS_DEPLOYMENT_STAGE']

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

    @staticmethod
    def debug_level() -> int:
        """
        Use the value returned by this method to conditionally enable verbose diagnostic output by the application,
        its daemons or during tests. A return value of

        * 0 should disable any verbose output
        * 1 should enable verbose output by application code
        * 2 should enable verbose output by the application and its dependencies
        """
        return int(os.environ.get('DSS_DEBUG', '0'))

    @classmethod
    def notification_is_async(cls) -> bool:
        """
        True, if notifications should be performed asynchronously and reliably (with retries on failures).

        False, if notifications should be performed synchronously and without any retries.
        """
        return cls.notification_attempts() > 0

    @classmethod
    def notification_attempts(cls) -> int:
        """
        The maximum number of asynchronous notification attempts or 0 if notifications should be performed
        synchronously.
        """
        attempts = os.environ.get('DSS_NOTIFY_ATTEMPTS')
        return int(attempts) if attempts else len(cls.notification_delays())

    @classmethod
    def notification_delays(cls) -> typing.List[float]:
        """
        A list of delays between asynchronous notification attempts. See :py:meth:`dss.notify.Notifier.__init__`.
        """
        return list(map(float, os.environ.get('DSS_NOTIFY_DELAYS', "").split()))

    @classmethod
    def notification_workers(cls) -> typing.Optional[int]:
        """
        The number of worker threads used by the asynchronous notifier or None if the number of workers should be
        determined automatically. See :py:meth:`dss.notify.Notifier.__init__`
        """
        value = os.environ.get('DSS_NOTIFY_WORKERS')
        return int(value) if value else None

    @classmethod
    def notification_timeout(cls) -> typing.Optional[float]:
        value = os.environ.get('DSS_NOTIFY_TIMEOUT')
        return float(value) if value else None

    @classmethod
    def get_elasticsearch_host(cls):
        return os.environ.get('DSS_ES_ENDPOINT', "localhost")

    @classmethod
    def get_elasticsearch_port(cls):
        return int(os.environ.get('DSS_ES_PORT', "443"))

    @classmethod
    def get_elasticsearch_timeout(cls):
        return int(os.environ.get('DSS_ES_TIMEOUT', "10"))


class Replica(Enum):
    aws = (Config.get_s3_bucket, Config.get_s3_checkout_bucket, "s3", S3BlobStore, S3HCABlobStore)
    gcp = (Config.get_gs_bucket, Config.get_gs_checkout_bucket, "gs", GSBlobStore, GSHCABlobStore)

    def __init__(
            self,
            bucket_getter: typing.Callable[[], str],
            checkout_bucket_getter: typing.Callable[[], str],
            storage_schema: str,
            blobstore_class: typing.Type[BlobStore],
            hcablobstore_class: typing.Type[HCABlobStore],
    ) -> None:
        self._bucket_getter = bucket_getter
        self._checkout_bucket_getter = checkout_bucket_getter
        self._storage_schema = storage_schema
        self._blobstore_class = blobstore_class
        self._hcablobstore_class = hcablobstore_class

    @property
    def bucket(self) -> str:
        return self._bucket_getter()

    @property
    def storage_schema(self) -> str:
        return self._storage_schema

    @property
    def blobstore_class(self) -> typing.Type[BlobStore]:
        return self._blobstore_class

    @property
    def hcablobstore_class(self) -> typing.Type[HCABlobStore]:
        return self._hcablobstore_class

    @property
    def checkout_bucket(self) -> str:
        return self._checkout_bucket_getter()

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

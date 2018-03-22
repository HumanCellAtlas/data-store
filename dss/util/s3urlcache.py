"""Utilities in this file are used to store and retrieve urls cached in an s3 bucket."""
import io
import boto3
from cloud_blobstore import BlobNotFoundError
import logging

from dss import Replica, Config
from hashlib import sha1
import requests


logger = logging.getLogger(__name__)


class SizeLimitError(IOError):
    def __init__(self, url, limit) -> None:
        super().__init__(f"{url} not cached. The URL's contents have exceeded {limit} bytes.")


class S3UrlCache:
    """
    Caches content of arbitrary URLs the first time they are requested. Currently only supports content lengths of up
    to a few megabytes.
    """
    _max_size_default = 64 * 1024 * 1024  # The default max_size per URL = 64 MB
    _chunk_size_default = 1024 * 1024  # The default chunk_size = 1 MB

    # The prefix of the keys used to store the cached URL contents.
    #
    _prefix = 'cache'

    # The version of the cache layout. Increment it whenever this code changes in a way that breaks compatibility
    # with existing cached URLs. This version will be appended to _prefix. Instruct operators to remove cached URL
    # keys from the caching bucket for all but the most recent version at some point after deploying this code.
    #
    _version = 1

    def __init__(self,
                 max_size: int = _max_size_default,
                 chunk_size: int = _chunk_size_default) -> None:
        """
        :param max_size: The maximum number of bytes of content that can be cached. An error is returned if the content
        is greater than max_size, and the URL is not cached.
        :param chunk_size: The amount of content retrieved from the URL per request."""
        self.max_size = max_size if max_size is not None else self._max_size_default
        self.chunk_size = chunk_size if chunk_size is not None else self._chunk_size_default
        self.s3_client = boto3.client('s3')

        # A URL's contents are only stored in S3 to keep the data closer to the aws lambas which use them.
        self.blobstore = Config.get_blobstore_handle(Replica.aws)
        self.bucket = Replica.aws.bucket

    def resolve(self, url: str) -> bytearray:
        """
        Requests the contents of a URL by first checking for the contents in an S3 bucket. If not present, the contents
        are retrieved from the URL and stored in S3.

        :param url: The url to retrieve the content from.
        """
        key = self._url_to_key(url)

        # if key in S3 bucket return value from there.
        try:
            content = bytearray(self.blobstore.get(self.bucket, key))
        except BlobNotFoundError:
            logger.info(f"{url} not found in cache. Adding it to {self.bucket} with key {key}.")
            with requests.get(url, stream=True) as resp_obj:
                resp_obj.raise_for_status()
                content = bytearray()
                for chunk in resp_obj.iter_content(chunk_size=self.chunk_size):
                    #  check if max_size exceeded before storing content to avoid storing large chunks
                    if len(content) + len(chunk) > self.max_size:
                        raise SizeLimitError(url, self.max_size)
                    content.extend(chunk)
            self._upload_content(key, url, content)
        return content

    def evict(self, url: str):
        """
        Removes the cached URL content from S3.
        :param url: the URL for the content to removed from S3
        """
        if self.contains(url):
            logger.info(f"{url} removed from cache in {self.bucket}.")
            self.blobstore.delete(self.bucket, self._url_to_key(url))
        else:
            logger.info(f"{url} not found and not removed from cache.")

    def contains(self, url: str) -> bool:
        key = self._url_to_key(url)
        try:
            self.blobstore.get_user_metadata(self.bucket, key)['dss_cached_url']
        except BlobNotFoundError:
            return False
        else:
            return True

    def _reverse_key_lookup(self, key: str) -> str:
        return self.blobstore.get_user_metadata(self.bucket, key)['dss_cached_url']

    @classmethod
    def _url_to_key(cls, url: str) -> str:
        hash = sha1(url.encode("utf-8")).hexdigest()
        return f'{cls._prefix}.{cls._version}/{hash}' + hash

    def _upload_content(self, key: str, url: str, content: bytearray) -> None:
        meta_data = {
            "Metadata": {'dss_cached_url': url},
            "ContentType": "application/octet-stream",
        }
        self.s3_client.upload_fileobj(
            Fileobj=io.BytesIO(content),
            Bucket=self.bucket,
            Key=key,
            ExtraArgs=meta_data,
        )

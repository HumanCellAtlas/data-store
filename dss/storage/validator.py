"""Utilities in this file are used to removed extra fields from index data before adding to the index."""
import io
from io import BytesIO

import boto3
from cloud_blobstore import BlobNotFoundError
from jsonschema import validators
from jsonschema import _utils, _validators

import json
from typing import List
import logging

from dss import Replica, Config
from hashlib import sha1
import requests


logger = logging.getLogger(__name__)


DSS_Draft4Validator = validators.create(meta_schema=_utils.load_schema("draft4"),
                                        validators={'$ref': _validators.ref,
                                                    'additionalProperties': _validators.additionalProperties,
                                                    'properties': _validators.properties_draft4,
                                                    'required': _validators.required_draft4
                                                    },
                                        version="draft4"
                                        )


class SizeLimitError(IOError):
    def __init__(self, url, limit) -> None:
        super().__init__(f"{url} not cached. The URL's contents have exceeded {limit} bytes.")


class S3UrlCache:
    """Caches content of arbitrary URLs the first time they are requested. Currently only supports content lengths of up
     to a few megabytes."""
    _max_size_default = 64 * 1024 * 1024  # The default max_size per URL = 64 MB
    _chunk_size_default = 1024 * 1024  # The default chunk_size = 1 MB

    def __init__(self,
                 max_size: int = _max_size_default,
                 chunk_size: int = _chunk_size_default):
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
            logger.info("%s", f"{url} not found in cache. Adding it to {self.bucket} with key {key}.")
            with requests.get(url, stream=True) as resp_obj:
                content = bytearray()
                for chunk in resp_obj.iter_content(chunk_size=self.chunk_size):
                    #  check if max_size exceeded before storing content to avoid storing large chunks
                    if len(content) + len(chunk) > self.max_size:
                        raise SizeLimitError(url, self.max_size)
                    content.extend(chunk)
            self._upload_content(key, url, content)
        return content

    def evict(self, url: str) -> bool:
        '''
        Removes the cached URL content from S3.
        :param url: the url for the content to removed from S3'''
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

    @staticmethod
    def _url_to_key(url: str) -> str:
        return 'cache/' + sha1(url.encode("utf-8")).hexdigest()

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


def remove_json_fields(json_data: dict, path: List[str], fields: List[str]) -> None:
    """
    Removes fields from the path in json_data.

    :param json_data: The JSON data from which to remove fields.
    :param path: A list of indices (either field names or array indices) forming a path through the JSON data.
    :param fields: A list of fields to remove from the JSON data at the location specified by path.
    """
    current = json_data
    for step in path:
        current = current[step]
    for field in fields:
        current.pop(field)


def scrub_index_data(index_data: dict, bundle_id: str) -> list:
    cache = S3UrlCache()

    def request_json(url):
        return json.loads(cache.resolve(url).decode("utf-8"))

    resolver = validators.RefResolver(referrer='',
                                      base_uri='',
                                      handlers={'http': request_json,
                                                'https': request_json}
                                      )
    extra_fields = []
    extra_documents = []
    for document in index_data.keys():
        core = index_data[document].get('core')
        schema_url = None if core is None else core.get('schema_url')
        if schema_url is not None:
            try:
                schema = request_json(schema_url)
            except Exception as ex:
                extra_documents.append(document)
                logger.warning("%s", f"Unable to retrieve schema from {document} in {bundle_id} "
                                     f"because retrieving {schema_url} caused exception: {ex}.")
            else:
                for error in DSS_Draft4Validator(schema, resolver=resolver).iter_errors(index_data[document]):
                    if error.validator == 'additionalProperties':
                        path = [document, *error.path]
                        #  Example error message: "Additional properties are not allowed ('extra_lst', 'extra_top' were
                        #  unexpected)" or "'extra', does not match any of the regexes: '^characteristics_.*$'"
                        fields_to_remove = (path, [field for field in _utils.find_additional_properties(error.instance,
                                                                                                        error.schema)])
                    extra_fields.append(fields_to_remove)
        else:
            logger.warning("%s", f"Unable to retrieve schema_url from {document} in {bundle_id} because "
                                 f"core.schema_url does not exist.")
            extra_documents.append(document)
    if extra_documents:
        extra_fields.append(([], extra_documents))
    removed_fields = []
    for path, fields in extra_fields:
        remove_json_fields(index_data, path, fields)
        removed_fields.extend(['.'.join((*path, field)) for field in fields])
    if removed_fields:
        logger.info(f"In {bundle_id}, unexpected additional fields have been removed from the data"
                    f" to be indexed. Removed {removed_fields}.")
    return removed_fields

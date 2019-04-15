import io
from functools import lru_cache
import json
import typing
import time

import cachetools
from cloud_blobstore import BlobNotFoundError, BlobStore
from cloud_blobstore.s3 import S3BlobStore

from dss import Config, Replica
from dss.storage.identifiers import DSS_BUNDLE_KEY_REGEX, DSS_BUNDLE_TOMBSTONE_REGEX, BundleTombstoneID, BundleFQID
from dss.storage.blobstore import test_object_exists
from dss.util import multipart_parallel_upload


_bundle_manifest_cache = cachetools.LRUCache(maxsize=4)


def get_bundle_manifest(
        uuid: str,
        replica: Replica,
        version: typing.Optional[str],
        *,
        bucket: typing.Optional[str] = None) -> typing.Optional[dict]:
    key = BundleFQID(uuid, version).to_key()
    if key in _bundle_manifest_cache:
        return _bundle_manifest_cache[key]
    else:
        bundle = _get_bundle_manifest(uuid, replica, version)
        if bundle is not None:
            _bundle_manifest_cache[key] = bundle
        return bundle


def _get_bundle_manifest(
        uuid: str,
        replica: Replica,
        version: typing.Optional[str],
        *,
        bucket: typing.Optional[str] = None) -> typing.Optional[dict]:
    """
    Return the contents of the bundle manifest file from cloud storage, subject to the rules of tombstoning.  If version
    is None, return the latest version, once again, subject to the rules of tombstoning.

    If the bundle cannot be found, return None
    """
    uuid = uuid.lower()

    handle = Config.get_blobstore_handle(replica)
    default_bucket = replica.bucket

    # need the ability to use fixture bucket for testing
    bucket = default_bucket if bucket is None else bucket

    def tombstone_exists(uuid: str, version: typing.Optional[str]):
        return test_object_exists(handle, bucket, BundleTombstoneID(uuid=uuid, version=version).to_key())

    # handle the following deletion cases
    # 1. the whole bundle is deleted
    # 2. the specific version of the bundle is deleted
    if tombstone_exists(uuid, None) or (version and tombstone_exists(uuid, version)):
        return None

    # handle the following deletion case
    # 3. no version is specified, we want the latest _non-deleted_ version
    if version is None:
        # list the files and find the one that is the most recent.
        prefix = f"bundles/{uuid}."
        object_names = handle.list(bucket, prefix)
        version = _latest_version_from_object_names(object_names)

    if version is None:
        # no matches!
        return None

    bundle_fqid = BundleFQID(uuid=uuid, version=version)

    # retrieve the bundle metadata.
    try:
        bundle_manifest_blob = handle.get(bucket, bundle_fqid.to_key()).decode("utf-8")
        return json.loads(bundle_manifest_blob)
    except BlobNotFoundError:
        return None


def idempotent_save(blobstore: BlobStore, bucket: str, key: str, data: dict) -> typing.Tuple[bool, bool]:
    """
    idempotent_save attempts to save an object to the BlobStore. Its return values indicate whether the save was made
    successfully and whether the operation could be completed idempotently. If the data in the blobstore does not match
    the data parameter, the data in the blobstore is _not_ overwritten.

    :param blobstore: the blobstore to save the data to
    :param bucket: the bucket in the blobstore to save the data to
    :param key: the key of the object to save
    :param data: the data to save
    :return: a tuple of booleans (was the data saved?, was the save idempotent?)
    """
    if test_object_exists(blobstore, bucket, key):
        # fetch the file metadata, compare it to what we have.
        existing_data = json.loads(blobstore.get(bucket, key).decode("utf-8"))
        return False, existing_data == data
    else:
        # write manifest to persistent store
        _d = json.dumps(data).encode("utf-8")
        part_size = 16 * 1024 * 1024
        if isinstance(blobstore, S3BlobStore) and len(_d) > part_size:
            with io.BytesIO(_d) as fh:
                multipart_parallel_upload(
                    blobstore.s3_client,
                    bucket,
                    key,
                    fh,
                    part_size=part_size,
                    parallelization_factor=20
                )
        else:
            blobstore.upload_file_handle(bucket, key, io.BytesIO(_d))

        if DSS_BUNDLE_KEY_REGEX.match(key) is not None:
            _bundle_manifest_cache[key] = data

        return True, True


def _latest_version_from_object_names(object_names: typing.Iterator[str]) -> str:
    dead_versions = set()  # type: typing.Set[str]
    all_versions = set()  # type: typing.Set[str]
    set_checks = [
        (DSS_BUNDLE_TOMBSTONE_REGEX, dead_versions),
        (DSS_BUNDLE_KEY_REGEX, all_versions),
    ]

    for object_name in object_names:
        for regex, version_set in set_checks:
            match = regex.match(object_name)
            if match:
                _, version = match.groups()
                version_set.add(version)
                break

    version = None

    for current_version in (all_versions - dead_versions):
        if version is None or current_version > version:
            version = current_version

    return version

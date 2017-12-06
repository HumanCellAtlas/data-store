import json
import typing

import re
from dss import Config, DSSException
from dss.hcablobstore import BundleFileMetadata, BundleMetadata
from dss.util import UrlBuilder
from dss.util.blobstore import test_object_exists
from cloud_blobstore import BlobNotFoundError

UUID_PATTERN = "[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-4[0-9A-Fa-f]{3}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}"
UUID_REGEX = re.compile(UUID_PATTERN)
VERSION_PATTERN = "\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}[.]\d{6}Z"
VERSION_REGEX = re.compile(VERSION_PATTERN)

# matches just bundle keys
DSS_BUNDLE_KEY_REGEX = re.compile(f"^bundles/({UUID_PATTERN})\.({VERSION_PATTERN})$")
# matches just bundle tombstones
DSS_BUNDLE_TOMBSTONE_REGEX = re.compile(f"^bundles/({UUID_PATTERN})(?:\.(" + VERSION_PATTERN + "))?" + "\.dead$")
# matches all bundle objects
DSS_OBJECT_NAME_REGEX = re.compile(f"^bundles/({UUID_PATTERN})(?:\.({VERSION_PATTERN}))?(?:\.dead)?$")


def get_bundle_from_bucket(
        uuid: str,
        replica: str,
        version: typing.Optional[str],
        bucket: typing.Optional[str],
        directurls: bool = False):
    uuid = uuid.lower()

    handle, hca_handle, default_bucket = Config.get_cloud_specific_handles(replica)

    # need the ability to use fixture bucket for testing
    bucket = default_bucket if bucket is None else bucket

    def exists(name):
        return test_object_exists(handle, bucket, name)

    # handle the following deletion cases
    # 1. the whole bundle is deleted
    # 2. the specific version of the bundle is deleted
    if exists(f"bundles/{uuid}.dead") or (version and exists(f"bundles/{uuid}.{version}.dead")):
        raise DSSException(404, "not_found", "EMPTY Cannot find file!")

    # handle the following deletion case
    # 3. no version is specified, we want the latest _non-deleted_ version
    if version is None:
        # list the files and find the one that is the most recent.
        prefix = f"bundles/{uuid}."
        object_names = handle.list(bucket, prefix)
        version = _latest_version_from_object_names(object_names)

    if version is None:
        # no matches!
        raise DSSException(404, "not_found", "Cannot find file!")

    # retrieve the bundle metadata.
    try:
        bundle_metadata = json.loads(
            handle.get(
                bucket,
                f"bundles/{uuid}.{version}"
            ).decode("utf-8"))
    except BlobNotFoundError:
        raise DSSException(404, "not_found", "Cannot find file!")

    filesresponse = []  # type: typing.List[dict]
    for file in bundle_metadata[BundleMetadata.FILES]:
        file_version = {
            'name': file[BundleFileMetadata.NAME],
            'content-type': file[BundleFileMetadata.CONTENT_TYPE],
            'size': file[BundleFileMetadata.SIZE],
            'uuid': file[BundleFileMetadata.UUID],
            'version': file[BundleFileMetadata.VERSION],
            'crc32c': file[BundleFileMetadata.CRC32C],
            's3_etag': file[BundleFileMetadata.S3_ETAG],
            'sha1': file[BundleFileMetadata.SHA1],
            'sha256': file[BundleFileMetadata.SHA256],
            'indexed': file[BundleFileMetadata.INDEXED],
        }
        if directurls:
            file_version['url'] = str(UrlBuilder().set(
                scheme=Config.get_storage_schema(replica),
                netloc=bucket,
                path="blobs/{}.{}.{}.{}".format(
                    file[BundleFileMetadata.SHA256],
                    file[BundleFileMetadata.SHA1],
                    file[BundleFileMetadata.S3_ETAG],
                    file[BundleFileMetadata.CRC32C],
                ),
            ))
        filesresponse.append(file_version)

    return dict(
        bundle=dict(
            uuid=uuid,
            version=version,
            files=filesresponse,
            creator_uid=bundle_metadata[BundleMetadata.CREATOR_UID],
        )
    )


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


def get_bundle(
        uuid: str,
        replica: str,
        version: str = None,
        directurls: bool = False):
    return get_bundle_from_bucket(
        uuid,
        replica,
        version,
        None,
        directurls
    )


def bundle_key_to_bundle_fqid(bundle_key: str) -> str:
    uuid, version = DSS_OBJECT_NAME_REGEX.match(bundle_key).groups()
    if version:
        return format_bundle_fqid(uuid, version)
    else:
        raise Exception(f"Object name does not contain a valid bundle identifier: {bundle_key}")


def format_bundle_fqid(uuid: str, version: str) -> str:
    if UUID_REGEX.match(uuid) and VERSION_REGEX.match(version):
        return f"{uuid}.{version}"
    else:
        raise Exception(f"Not a valid version regex-version pair: {uuid}, {version}")

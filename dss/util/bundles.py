import json
import typing

from cloud_blobstore import BlobNotFoundError

from dss import Config, DSSException, Replica
from dss.hcablobstore import BundleFileMetadata, BundleMetadata
from dss.storage.bundles import DSS_BUNDLE_KEY_REGEX, DSS_BUNDLE_TOMBSTONE_REGEX, TombstoneID, BundleFQID
from dss.util import UrlBuilder
from dss.util.blobstore import test_object_exists


def get_bundle_from_bucket(
        uuid: str,
        replica: Replica,
        version: typing.Optional[str],
        bucket: typing.Optional[str],
        directurls: bool=False):
    uuid = uuid.lower()

    handle, hca_handle, default_bucket = Config.get_cloud_specific_handles_DEPRECATED(replica)

    # need the ability to use fixture bucket for testing
    bucket = default_bucket if bucket is None else bucket

    def tombstone_exists(uuid: str, version: typing.Optional[str]):
        return test_object_exists(handle, bucket, TombstoneID(uuid=uuid, version=version).to_key())

    # handle the following deletion cases
    # 1. the whole bundle is deleted
    # 2. the specific version of the bundle is deleted
    if tombstone_exists(uuid, None) or (version and tombstone_exists(uuid, version)):
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

    bundle_fqid = BundleFQID(uuid=uuid, version=version)

    # retrieve the bundle metadata.
    try:
        bundle_metadata = json.loads(
            handle.get(
                bucket,
                bundle_fqid.to_key(),
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
                scheme=replica.storage_schema,
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
        replica: Replica,
        version: str=None,
        directurls: bool=False):
    return get_bundle_from_bucket(
        uuid,
        replica,
        version,
        None,
        directurls
    )

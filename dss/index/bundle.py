import json
import logging
from typing import Iterable, Mapping, Optional, Sequence

from cloud_blobstore import BlobNotFoundError, BlobStoreError

from dss import Config, Replica
from dss.storage.hcablobstore import BundleFileMetadata, BundleMetadata
from dss.storage.identifiers import BundleFQID, ObjectIdentifier, TombstoneID
from dss.util import create_blob_key
from dss.util.types import JSON

logger = logging.getLogger(__name__)


class Bundle:
    """
    In the context of indexing, a bundle is a collection of metadata files stored in a given replica. It is uniquely
    identified by an FQID. One of the metadata files, the bundle manifest, is a description of the remaining files.
    Note that indexeing is agnostic to the concept of bundles having multiple versions: instances of this class
    always represent a siongle bundle version and each version of a conceptual bundle is represented by a separate
    instance of this class.
    """

    def __init__(self, replica: Replica, fqid: BundleFQID, manifest: JSON, files: Mapping[str, JSON]) -> None:
        self.replica = replica
        self.fqid = fqid
        self.manifest = manifest
        self.files = files

    @classmethod
    def from_replica(cls, replica: Replica, fqid: BundleFQID):
        manifest = cls._read_bundle_manifest(replica, fqid)
        files = cls._read_file_infos(replica, fqid, manifest)
        self = cls(replica, fqid, manifest=manifest, files=files)
        return self

    @classmethod
    def _read_bundle_manifest(cls, replica: Replica, fqid: BundleFQID) -> dict:
        handle = Config.get_blobstore_handle(replica)
        bucket_name = replica.bucket
        manifest_string = handle.get(bucket_name, fqid.to_key()).decode("utf-8")
        logger.debug("Read bundle manifest from bucket %s with bundle key %s: %s",
                     bucket_name, fqid.to_key(), manifest_string)
        manifest = json.loads(manifest_string, encoding="utf-8")
        return manifest

    @classmethod
    def _read_file_infos(cls, replica: Replica, fqid: BundleFQID, manifest: JSON) -> Mapping[str, JSON]:
        handle = Config.get_blobstore_handle(replica)
        bucket_name = replica.bucket
        files_info = manifest[BundleMetadata.FILES]
        assert isinstance(files_info, list)
        index_files = {}
        for file_info in files_info:
            if file_info[BundleFileMetadata.INDEXED] is True:
                content_type = file_info[BundleFileMetadata.CONTENT_TYPE]
                file_name = file_info[BundleFileMetadata.NAME]
                if not content_type.startswith('application/json'):
                    logger.warning(f"In bundle {fqid} the file '{file_name}' is marked for indexing yet has "
                                   f"content type '{content_type}' instead of the required content type "
                                   f"'application/json'. This file will not be indexed.")
                    continue
                file_blob_key = create_blob_key(file_info)
                try:
                    file_string = handle.get(bucket_name, file_blob_key).decode("utf-8")
                    file_json = json.loads(file_string)
                # TODO (mbaumann) Are there other JSON-related exceptions that should be checked below?
                except json.decoder.JSONDecodeError as ex:
                    logger.warning(f"In bundle {fqid} the file '{file_name}' is marked for indexing yet could "
                                   f"not be parsed. This file will not be indexed. Exception: {ex}")
                    continue
                except BlobStoreError as ex:
                    exception_type = type(ex).__name__
                    logger.warning(f"In bundle {fqid} the file '{file_name}' is marked for indexing yet could "
                                   f"not be accessed. This file will not be indexed. Exception: {exception_type}, "
                                   f"file blob key: {file_blob_key}")
                    continue
                logger.debug(f"Loaded file: {file_name}")
                index_files[file_name] = file_json
        return index_files

    def lookup_tombstone(self) -> Optional['Tombstone']:
        """
        Return the tombstone placed on this bundle in storage or None if none exists.
        """
        for all_versions in (False, True):
            tombstone_id = self.fqid.to_tombstone_id(all_versions=all_versions)
            try:
                return Tombstone.from_replica(self.replica, tombstone_id)
            except BlobNotFoundError:
                pass
        return None


class Tombstone:
    """
    A tombstone is a storage object whose FQID matches that of a given single bundle or all bundles for a given UUID.
    Bundles for which there is a tombstone must be omitted from the index.
    """
    def __init__(self, replica: Replica, fqid: TombstoneID, body: JSON) -> None:
        self.replica = replica
        self.fqid = fqid
        self.body = body

    @classmethod
    def from_replica(cls, replica: Replica, tombstone_id: TombstoneID):
        blobstore = Config.get_blobstore_handle(replica)
        bucket_name = replica.bucket
        body = json.loads(blobstore.get(bucket_name, tombstone_id.to_key()))
        self = cls(replica, tombstone_id, body)
        return self

    def list_dead_bundles(self) -> Sequence[Bundle]:
        blobstore = Config.get_blobstore_handle(self.replica)
        bucket_name = self.replica.bucket
        assert isinstance(self.fqid, TombstoneID)
        if self.fqid.is_fully_qualified():
            # If a version is specified, delete just that bundle …
            bundle_fqids: Iterable[BundleFQID] = [self.fqid.to_bundle_fqid()]
        else:
            # … otherwise, delete all bundles with the same UUID from the index.
            prefix = self.fqid.to_key_prefix()
            fqids = [ObjectIdentifier.from_key(k) for k in set(blobstore.list(bucket_name, prefix))]
            bundle_fqids = filter(lambda fqid: type(fqid) == BundleFQID, fqids)
        bundles = [Bundle.from_replica(self.replica, bundle_fqid) for bundle_fqid in bundle_fqids]
        return bundles

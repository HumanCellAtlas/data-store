import os
import json
import logging
import threading
import typing
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

from flashflood import FlashFlood, FlashFloodEventNotFound

from dss.util.aws import resources
from dss.config import Config, Replica
from dss.storage.identifiers import TOMBSTONE_SUFFIX
from dss.util.version import datetime_from_timestamp

# TODO: What happens when an event is recorder with timestamp erlier tha latest journal?

@lru_cache(maxsize=2)
def get_bundle_metadata_document(replica: Replica,
                                 key: str,
                                 flashflood_prefix: str=None) -> dict:
    if key.endswith(TOMBSTONE_SUFFIX):
        return _build_bundle_metadata_document(replica, key)
    else:
        fqid = key.split("/", 1)[1]
        pfx = flashflood_prefix or replica.flashflood_prefix_read
        ff = FlashFlood(resources.s3, Config.get_flashflood_bucket(), pfx)
        try:
            metadata_document = json.loads(ff.get_event(fqid).data.decode("utf-8"))
        except FlashFloodEventNotFound:
            metadata_document = None
        return metadata_document

@lru_cache(maxsize=2)
def get_deleted_bundle_metadata_document(replica: Replica, key: str) -> dict:
    _, fqid = key.split("/")
    uuid, version = fqid.split(".", 1)
    return {
        'event_type': "DELETE",
        "uuid": uuid,
        "version": version,
    }

# TODO: Delete event from flashflood
# TODO: Update event data in flashflood

@lru_cache(maxsize=2)
def record_event_for_bundle(replica: Replica,
                            key: str,
                            flashflood_prefixes: typing.Tuple[str, ...]=None,
                            use_version_for_timestamp: bool=False) -> dict:
    """
    Build the bundle metadata document, record it into flashflood, and return it
    """
    # TODO: Add support for unversioned tombstones
    fqid = key.split("/", 1)[1]
    if flashflood_prefixes is None:
        flashflood_prefixes = replica.flashflood_prefix_write
    metadata_document = _build_bundle_metadata_document(replica, key)
    if use_version_for_timestamp:
        _, version = fqid.split(".", 1)
        event_date = datetime_from_timestamp(version)
    else:
        event_date = datetime.utcnow()
    for pfx in flashflood_prefixes:
        ff = FlashFlood(resources.s3, Config.get_flashflood_bucket(), pfx)
        if not ff.event_exists(fqid):
            ff.put(json.dumps(metadata_document).encode("utf-8"), event_id=fqid, date=event_date)
    return metadata_document

def _build_bundle_metadata_document(replica: Replica, key: str) -> dict:
    """
    This returns a JSON document with bundle manifest and metadata files suitable for JMESPath filters.
    """
    handle = Config.get_blobstore_handle(replica)
    manifest = json.loads(handle.get(replica.bucket, key).decode("utf-8"))
    if key.endswith(TOMBSTONE_SUFFIX):
        manifest['event_type'] = "TOMBSTONE"
        return manifest
    else:
        lock = threading.Lock()
        files: dict = defaultdict(list)

        def _read_file(file_metadata):
            blob_key = "blobs/{}.{}.{}.{}".format(
                file_metadata['sha256'],
                file_metadata['sha1'],
                file_metadata['s3-etag'],
                file_metadata['crc32c'],
            )
            contents = handle.get(replica.bucket, blob_key).decode("utf-8")
            try:
                file_info = json.loads(contents)
            except json.decoder.JSONDecodeError:
                logging.info(f"{file_metadata['name']} not json decodable")
            else:
                # Modify name to avoid confusion with JMESPath syntax
                name = _dot_to_underscore_and_strip_numeric_suffix(file_metadata['name'])
                with lock:
                    files[name].append(file_info)

        # TODO: Consider scaling parallelization with Lambda size
        with ThreadPoolExecutor(max_workers=4) as e:
            e.map(_read_file, [file_metadata for file_metadata in manifest['files']
                               if file_metadata['content-type'].startswith("application/json")])

        return {
            'event_type': "CREATE",
            'manifest': manifest,
            'files': dict(files),
        }

def _dot_to_underscore_and_strip_numeric_suffix(name: str) -> str:
    """
    e.g. "library_preparation_protocol_0.json" -> "library_preparation_protocol_json"
    """
    name = name.replace('.', '_')
    if name.endswith('_json'):
        name = name[:-5]
        parts = name.rpartition("_")
        if name != parts[2]:
            name = parts[0]
        name += "_json"
    return name

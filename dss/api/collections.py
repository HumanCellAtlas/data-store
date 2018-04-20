import os, sys, json, logging, datetime, io
from typing import List
from uuid import uuid4

import requests
from flask import jsonify, request
import jsonpointer

from dss import Config, Replica
from dss.error import DSSException, dss_handler
from dss.storage.hcablobstore import FileMetadata, HCABlobStore

from cloud_blobstore import BlobNotFoundError, BlobStoreUnknownError

logger = logging.getLogger(__name__)
dss_bucket = Config.get_s3_bucket()

@dss_handler
def get(uuid: str, version: str, replica: str):
    authenticated_user_email = request.token_info['email']
    replica = Replica[replica]
    handle = Config.get_blobstore_handle(replica)
    if version is None:
        query prefix
    return json.loads(handle.get(replica.bucket, "collections/{}.{}".format(uuid, version)))

@dss_handler
def find(replica: str):
    owner = request.token_info['email']

    return jsonify({}), requests.codes.okay

@dss_handler
def put(json_request_body: dict, replica: str, uuid: str, version: str):
    print("Will put:")
    print(json.dumps(json_request_body))
    replica = Replica[replica]
    handle = Config.get_blobstore_handle(replica)
    verify_collection(json_request_body["contents"], replica, handle)
    collection_uuid = uuid if uuid else str(uuid4())
    collection_version = version if version else "0"
    handle.upload_file_handle(replica.bucket,
                              "collections/{}.{}".format(collection_uuid, collection_version),
                              io.BytesIO(json.dumps(json_request_body).encode("utf-8")))
    return jsonify(dict(uuid=collection_uuid, version=collection_version)), requests.codes.created

@dss_handler
def patch(json_request_body: dict, replica: str):
    return jsonify({}), requests.codes.created

@dss_handler
def delete(uuid: str, replica: str):
    authenticated_user_email = request.token_info['email']
    #
    return jsonify({}), requests.codes.okay

def get_json_metadata(entity_type: str, uuid: str, version: str, replica: Replica, blobstore_handle: HCABlobStore):
    try:
        return json.loads(blobstore_handle.get(
            replica.bucket,
            "{}s/{}.{}".format(entity_type, uuid, version)))
    except BlobNotFoundError as ex:
        raise DSSException(404, "not_found", "Cannot find {}".format(entity_type))

def verify_collection(contents: List[dict], replica: Replica, blobstore_handle: HCABlobStore):
    """
    Given user-supplied collection contents that pass schema validation, resolve all entities in the collection and
    verify they exist.
    """
    for item in contents:
        # Item type could be bundle (default), file, collection, or other. If other, fragment must be set.
        # Retrieve the file metadata.
        try:
            if item["type"] in {"file", "bundle", "collection"}:
                item_metadata = get_json_metadata(item["type"], item["uuid"], item["version"], replica, blobstore_handle)
            else:
                item_metadata = get_json_metadata("file", item["uuid"], item["version"], replica, blobstore_handle)
                if "fragment" not in item:
                    raise Exception('The "fragment" field is required in collection elements other than files, bundles, and collections')
                blob_path = "blobs/" + ".".join((
                    item_metadata[FileMetadata.SHA256],
                    item_metadata[FileMetadata.SHA1],
                    item_metadata[FileMetadata.S3_ETAG],
                    item_metadata[FileMetadata.CRC32C],
                ))
                # check that item is marked as metadata, is json, and is less than max size
                item_doc = json.loads(blobstore_handle.get(replica.bucket, blob_path))
                item_content = jsonpointer.resolve_pointer(item_doc, item["fragment"])
        except Exception as e:
            raise DSSException(
                422,
                "invalid_link",
                'Error while parsing the link "{}": {}: {}'.format(item, type(e).__name__, e)
            )

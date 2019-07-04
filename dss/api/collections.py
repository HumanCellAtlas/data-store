import json, logging, datetime, io, functools
from typing import List
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from collections import OrderedDict

import requests
from flask import jsonify, request, make_response
import jsonpointer

from dss import Config, Replica
from dss.error import DSSException, dss_handler
from dss.storage.blobstore import test_object_exists
from dss.storage.hcablobstore import BlobStore, compose_blob_key
from dss.storage.identifiers import CollectionFQID, CollectionTombstoneID, COLLECTION_PREFIX
from dss.util import security, hashabledict, UrlBuilder
from dss.util.version import datetime_to_version_format
from dss.storage.blobstore import idempotent_save
from dss.collections import owner_lookup
from cloud_blobstore import BlobNotFoundError

MAX_METADATA_SIZE = 1024 * 1024

logger = logging.getLogger(__name__)


def get_impl(uuid: str, replica: str, version: str = None):
    uuid = uuid.lower()
    bucket = Replica[replica].bucket
    handle = Config.get_blobstore_handle(Replica[replica])

    tombstone_key = CollectionTombstoneID(uuid, version=None).to_key()
    if test_object_exists(handle, bucket, tombstone_key):
        raise DSSException(404, "not_found", "Could not find collection for UUID {}".format(uuid))

    if version is None:
        # list the collections and find the one that is the most recent.
        prefix = CollectionFQID(uuid, version=None).to_key_prefix()
        for matching_key in handle.list(bucket, prefix):
            matching_key = matching_key[len(prefix):]
            if version is None or matching_key > version:
                version = matching_key
    try:
        collection_blob = handle.get(bucket, CollectionFQID(uuid, version).to_key())
    except BlobNotFoundError:
        raise DSSException(404, "not_found", "Could not find collection for UUID {}".format(uuid))
    return json.loads(collection_blob)


@dss_handler
@security.authorized_group_required(['hca'])
def list_collections(per_page: int, start_at: int = 0):
    """
    Return a list of a user's collections.

    Collection uuids are indexed and called by the user's email in a dynamoDB table.

    :param int per_page: # of collections returned per paged response.
    :param int start_at: Where the next chunk of paged response should start at.
    :return: A dictionary containing a list of dictionaries looking like:
        {'collections': [{'uuid': uuid, 'version': version}, {'uuid': uuid, 'version': version}, ... , ...]}
    """
    # TODO: Replica is unused, so this does not use replica.  Appropriate?
    owner = security.get_token_email(request.token_info)

    collections = []
    for collection in owner_lookup.get_collection_fqids_for_owner(owner):
        fqid = CollectionFQID.from_key(f'{COLLECTION_PREFIX}/{collection}')
        collections.append({'uuid': fqid.uuid, 'version': fqid.version})

    # paged response
    if len(collections) - start_at > per_page:
        next_url = UrlBuilder(request.url)
        next_url.replace_query("start_at", str(start_at + per_page))
        collection_page = collections[start_at:start_at + per_page]
        response = make_response(jsonify({'collections': collection_page}), requests.codes.partial)
        response.headers['Link'] = f"<{next_url}>; rel='next'"
        return response
    # single response returning all collections (or those remaining)
    else:
        collection_page = collections[start_at:]
        return jsonify({'collections': collection_page}), requests.codes.ok


@dss_handler
@security.authorized_group_required(['hca'])
def get(uuid: str, replica: str, version: str = None):
    authenticated_user_email = security.get_token_email(request.token_info)
    collection_body = get_impl(uuid=uuid, replica=replica, version=version)
    if collection_body["owner"] != authenticated_user_email:
        raise DSSException(requests.codes.forbidden, "forbidden", f"Collection access denied")
    return collection_body


@dss_handler
@security.authorized_group_required(['hca'])
def put(json_request_body: dict, replica: str, uuid: str, version: str):
    authenticated_user_email = security.get_token_email(request.token_info)
    collection_body = dict(json_request_body, owner=authenticated_user_email)
    uuid = uuid.lower()
    handle = Config.get_blobstore_handle(Replica[replica])
    collection_body["contents"] = _dedpuplicate_contents(collection_body["contents"])
    verify_collection(collection_body["contents"], Replica[replica], handle)
    collection_uuid = uuid if uuid else str(uuid4())
    if version is None:
        timestamp = datetime.datetime.utcnow()
        version = datetime_to_version_format(timestamp)
    collection_version = version
    # update dynamoDB; used to speed up lookup time; will not update if owner already associated w/uuid
    owner_lookup.put_collection(owner=authenticated_user_email,
                                collection_fqid=str(CollectionFQID(collection_uuid, collection_version)))
    # add the collection file to the bucket
    handle.upload_file_handle(Replica[replica].bucket,
                              CollectionFQID(collection_uuid, collection_version).to_key(),
                              io.BytesIO(json.dumps(collection_body).encode("utf-8")))
    return jsonify(dict(uuid=collection_uuid, version=collection_version)), requests.codes.created


@dss_handler
@security.authorized_group_required(['hca'])
def patch(uuid: str, json_request_body: dict, replica: str, version: str):
    authenticated_user_email = security.get_token_email(request.token_info)

    uuid = uuid.lower()
    owner = get_impl(uuid=uuid, replica=replica)["owner"]
    if owner != authenticated_user_email:
        raise DSSException(requests.codes.forbidden, "forbidden", f"Collection access denied")

    handle = Config.get_blobstore_handle(Replica[replica])
    try:
        cur_collection_blob = handle.get(Replica[replica].bucket, CollectionFQID(uuid, version).to_key())
    except BlobNotFoundError:
        raise DSSException(404, "not_found", "Could not find collection for UUID {}".format(uuid))
    collection = json.loads(cur_collection_blob)
    for field in "name", "description", "details":
        if field in json_request_body:
            collection[field] = json_request_body[field]
    remove_contents_set = set(map(hashabledict, json_request_body.get("remove_contents", [])))
    collection["contents"] = [i for i in collection["contents"] if hashabledict(i) not in remove_contents_set]
    verify_collection(json_request_body.get("add_contents", []), Replica[replica], handle)
    collection["contents"].extend(json_request_body.get("add_contents", []))
    collection["contents"] = _dedpuplicate_contents(collection["contents"])
    timestamp = datetime.datetime.utcnow()
    new_collection_version = datetime_to_version_format(timestamp)
    handle.upload_file_handle(Replica[replica].bucket,
                              CollectionFQID(uuid, new_collection_version).to_key(),
                              io.BytesIO(json.dumps(collection).encode("utf-8")))
    return jsonify(dict(uuid=uuid, version=new_collection_version)), requests.codes.ok


def _dedpuplicate_contents(contents: List) -> List:
    dedup_collection: OrderedDict[int, dict] = OrderedDict()
    for item in contents:
        dedup_collection[hash(tuple(sorted(item.items())))] = item
    return list(dedup_collection.values())


@dss_handler
@security.authorized_group_required(['hca'])
def delete(uuid: str, replica: str):
    authenticated_user_email = security.get_token_email(request.token_info)

    uuid = uuid.lower()
    tombstone_key = CollectionTombstoneID(uuid, version=None).to_key()

    tombstone_object_data = dict(email=authenticated_user_email)

    owner = get_impl(uuid=uuid, replica=replica)["owner"]
    if owner != authenticated_user_email:
        raise DSSException(requests.codes.forbidden, "forbidden", f"Collection access denied")

    created, idempotent = idempotent_save(
        Config.get_blobstore_handle(Replica[replica]),
        Replica[replica].bucket,
        tombstone_key,
        json.dumps(tombstone_object_data).encode("utf-8")
    )
    if not idempotent:
        raise DSSException(requests.codes.conflict,
                           f"collection_tombstone_already_exists",
                           f"collection tombstone with UUID {uuid} already exists")
    status_code = requests.codes.ok
    response_body = dict()  # type: dict
    # update dynamoDB
    owner_lookup.delete_collection_uuid(owner=authenticated_user_email, uuid=uuid)
    return jsonify(response_body), status_code


@functools.lru_cache(maxsize=64)
def get_json_metadata(entity_type: str,
                      uuid: str,
                      version: str,
                      replica: Replica,
                      blobstore_handle: BlobStore,
                      max_metadata_size: int=MAX_METADATA_SIZE):
    try:
        key = "{}s/{}.{}".format(entity_type, uuid, version)
        # TODO: verify that file is a metadata file
        size = blobstore_handle.get_size(replica.bucket, key)
        if size > max_metadata_size:
            raise DSSException(
                requests.codes.unprocessable_entity,
                "invalid_link",
                "The file UUID {} refers to a file that is too large to process".format(uuid))
        return json.loads(blobstore_handle.get(
            replica.bucket,
            "{}s/{}.{}".format(entity_type, uuid, version)))
    except BlobNotFoundError:
        raise DSSException(
            requests.codes.unprocessable_entity,
            "invalid_link",
            "Could not find file for UUID {}".format(uuid))


def resolve_content_item(replica: Replica, blobstore_handle: BlobStore, item: dict):
    try:
        if item["type"] in {"file", "bundle", "collection"}:
            item_metadata = get_json_metadata(item["type"], item["uuid"], item["version"], replica, blobstore_handle)
        else:
            item_metadata = get_json_metadata("file", item["uuid"], item["version"], replica, blobstore_handle)
            if "fragment" not in item:
                raise Exception('The "fragment" field is required in collection elements '
                                'other than files, bundles, and collections')
            blob_path = compose_blob_key(item_metadata)
            # check that item is marked as metadata, is json, and is less than max size
            item_doc = json.loads(blobstore_handle.get(replica.bucket, blob_path))
            item_content = jsonpointer.resolve_pointer(item_doc, item["fragment"])
            return item_content
    except DSSException:
        raise
    except Exception as e:
        raise DSSException(
            requests.codes.unprocessable_entity,
            "invalid_link",
            'Error while parsing the link "{}": {}: {}'.format(item, type(e).__name__, e)
        )


def verify_collection(contents: List[dict], replica: Replica, blobstore_handle: BlobStore, batch_size=64):
    """
    Given user-supplied collection contents that pass schema validation, resolve all entities in the collection and
    verify they exist.
    """
    verifier = partial(resolve_content_item, replica, blobstore_handle)
    for i in range(0, len(contents), batch_size):
        with ThreadPoolExecutor(max_workers=20) as e:
            for result in e.map(verifier, contents[i:i + batch_size]):
                pass

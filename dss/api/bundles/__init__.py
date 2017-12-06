import datetime
import io
import json
import time

import iso8601
import nestedcontext
import requests
from cloud_blobstore import BlobNotFoundError, BlobStore
from flask import jsonify

from ...util.blobstore import test_object_exists
from ...util.bundles import get_bundle
from ...util.version import datetime_to_version_format
from ... import DSSException, dss_handler
from ...config import Config
from ...hcablobstore import BundleFileMetadata, BundleMetadata, FileMetadata

PUT_TIME_ALLOWANCE_SECONDS = 10
"""This is the minimum amount of time remaining on the lambda for us to retry on a PUT /bundles request."""


@dss_handler
def get(uuid: str,
        replica: str,
        version: str=None,
        # TODO: (ttung) once we can run the endpoints from each cloud, we should default to the local cloud.
        directurls: bool = False):
    return get_bundle(uuid, replica, version, directurls)


@dss_handler
def list_versions(uuid: str):
    return ["2014-10-23T00:35:14.800221Z"]


@dss_handler
def post():
    pass


@dss_handler
def put(uuid: str, replica: str, json_request_body: dict, version: str = None):
    uuid = uuid.lower()
    if version is not None:
        # convert it to date-time so we can format exactly as the system requires (with microsecond precision)
        timestamp = iso8601.parse_date(version)
    else:
        timestamp = datetime.datetime.utcnow()
    version = datetime_to_version_format(timestamp)

    handle, hca_handle, bucket = Config.get_cloud_specific_handles(replica)

    # what's the target object name for the bundle manifest?
    bundle_manifest_object_name = _format_bundle_prefix(uuid, version)

    # decode the list of files.
    files = [{'user_supplied_metadata': file}
             for file in json_request_body['files']]

    time_left = nestedcontext.inject("time_left")

    while True:  # each time through the outer while-loop, we try to gather up all the file metadata.
        for file in files:
            user_supplied_metadata = file['user_supplied_metadata']
            metadata_path = 'files/{}.{}'.format(user_supplied_metadata['uuid'], user_supplied_metadata['version'])
            if 'file_metadata' not in file:
                try:
                    file_metadata = handle.get(bucket, metadata_path)
                except BlobNotFoundError:
                    continue
                file['file_metadata'] = json.loads(file_metadata)

        # check to see if any file metadata is still not yet loaded.
        for file in files:
            if 'file_metadata' not in file:
                missing_file_user_metadata = file['user_supplied_metadata']
                break
        else:
            break

        # if we're out of time, give up.
        if time_left() > PUT_TIME_ALLOWANCE_SECONDS:
            time.sleep(1)
            continue

        raise DSSException(
            requests.codes.conflict,
            "file_missing",
            f"Could not find file {missing_file_user_metadata['uuid']}/{missing_file_user_metadata['version']}."
        )

    # TODO: (ttung) should validate the files' bundle UUID points back at us.

    # build a manifest consisting of all the files.
    bundle_metadata = {
        BundleMetadata.FORMAT: BundleMetadata.FILE_FORMAT_VERSION,
        BundleMetadata.VERSION: version,
        BundleMetadata.FILES: [
            {
                BundleFileMetadata.NAME: file['user_supplied_metadata']['name'],
                BundleFileMetadata.UUID: file['user_supplied_metadata']['uuid'],
                BundleFileMetadata.VERSION: file['user_supplied_metadata']['version'],
                BundleFileMetadata.CONTENT_TYPE: file['file_metadata'][FileMetadata.CONTENT_TYPE],
                BundleFileMetadata.SIZE: file['file_metadata'][FileMetadata.SIZE],
                BundleFileMetadata.INDEXED: file['user_supplied_metadata']['indexed'],
                BundleFileMetadata.CRC32C: file['file_metadata'][FileMetadata.CRC32C],
                BundleFileMetadata.S3_ETAG: file['file_metadata'][FileMetadata.S3_ETAG],
                BundleFileMetadata.SHA1: file['file_metadata'][FileMetadata.SHA1],
                BundleFileMetadata.SHA256: file['file_metadata'][FileMetadata.SHA256],
            }
            for file in files
        ],
        BundleMetadata.CREATOR_UID: json_request_body['creator_uid'],
    }

    was_created = _save_object_if_dne(
        handle,
        bucket,
        bundle_manifest_object_name,
        bundle_metadata,
        "bundle_already_exists",
        f"bundle with UUID {uuid} and version {version} already exists"
    )
    status_code = requests.codes.created if was_created else requests.codes.ok

    return jsonify(dict(version=version)), status_code


@dss_handler
def delete(uuid: str, replica: str, json_request_body: dict, version: str = None):
    uuid = uuid.lower()
    version = datetime_to_version_format(iso8601.parse_date(version)) if version else None

    bundle_prefix = _format_bundle_prefix(uuid, version)
    tombstone_object_name = bundle_prefix + '.dead'
    tombstone_object_data = _create_tombstone_data(
        creator_uid=json_request_body['creator_uid'],
        version=version,
        reason=json_request_body.get('reason'),
    )

    handle, hca_handle, bucket = Config.get_cloud_specific_handles(replica)

    if test_object_exists(handle, bucket, bundle_prefix, test_type='prefix'):
        _save_object_if_dne(
            handle,
            bucket,
            tombstone_object_name,
            tombstone_object_data,
            f"bundle_tombstone_already_exists",
            f"bundle tombstone with UUID {uuid} and version {version} already exists",
        )
        status_code = requests.codes.ok
        response_body = dict()  # type: dict
    else:
        status_code = requests.codes.not_found
        response_body = dict(title="bundle not found")

    return jsonify(response_body), status_code


def _format_bundle_prefix(uuid: str, version: str) -> str:
    if version:
        return f"bundles/{uuid}.{version}"
    else:
        return f"bundles/{uuid}"


def _save_object_if_dne(
        handle: BlobStore,
        bucket: str,
        object_name: str,
        data: dict,
        error_code: str,
        error_title: str) -> bool:
    if test_object_exists(handle, bucket, object_name):
        # fetch the file metadata, compare it to what we have.
        existing_data = json.loads(handle.get(bucket, object_name).decode("utf-8"))
        if existing_data != data:
            raise DSSException(
                requests.codes.conflict,
                error_code,
                error_title)
        return False
    else:
        # write manifest to persistent store
        handle.upload_file_handle(
            bucket,
            object_name,
            io.BytesIO(json.dumps(data).encode("utf-8")))
        return True


def _create_tombstone_data(creator_uid: str, version: str, reason: str) -> dict:
    # Future-proofing the case in which garbage collection is added
    data = dict(
        creator_uid=creator_uid,
        admin_deleted=True,
    )
    # optional params
    if reason is not None:
        data.update(reason=reason)
    if version is not None:
        data.update(version=version)
    return data

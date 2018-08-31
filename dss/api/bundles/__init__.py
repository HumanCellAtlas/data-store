import io
import os
import json
import time
import typing

import iso8601
import nestedcontext
import requests
from cloud_blobstore import BlobNotFoundError, BlobStore, BlobStoreTimeoutError
from flask import jsonify, redirect, request

from dss import DSSException, dss_handler
from dss.config import Config, Replica
from dss.storage.blobstore import test_object_exists, ObjectTest
from dss.storage.bundles import get_bundle_manifest
from dss.storage.checkout import CheckoutError, TokenError
from dss.storage.checkout.bundle import get_dst_bundle_prefix, verify_checkout
from dss.storage.identifiers import BundleTombstoneID, BundleFQID, FileFQID
from dss.storage.hcablobstore import BundleFileMetadata, BundleMetadata, FileMetadata
from dss.util import UrlBuilder
from dss.util.version import datetime_to_version_format

"""The retry-after interval in seconds. Sets up downstream libraries / users to
retry request after the specified interval."""
RETRY_AFTER_INTERVAL = 10


PUT_TIME_ALLOWANCE_SECONDS = 10
"""This is the minimum amount of time remaining on the lambda for us to retry on a PUT /bundles request."""

ADMIN_USER_EMAILS = set(os.environ['ADMIN_USER_EMAILS'].split(','))


@dss_handler
def get(
        uuid: str,
        replica: str,
        version: str=None,
        directurls: bool=False,
        presignedurls: bool=False,
        token: str=None,
):
    if directurls and presignedurls:
        raise DSSException(
            requests.codes.bad_request, "only_one_urltype", "only enable one of `directurls` or `presignedurls`")

    _replica = Replica[replica]
    bundle_metadata = get_bundle_manifest(uuid, _replica, version)
    if bundle_metadata is None:
        raise DSSException(404, "not_found", "Cannot find bundle!")

    if directurls or presignedurls:
        try:
            token, ready = verify_checkout(_replica, uuid, version, token)
        except TokenError as ex:
            raise DSSException(requests.codes.bad_request, "illegal_token", "Could not understand token", ex)
        except CheckoutError as ex:
            raise DSSException(requests.codes.server_error, "checkout_error", "Could not complete checkout", ex)
        if not ready:
            builder = UrlBuilder(request.url)
            builder.replace_query("token", token)
            response = redirect(str(builder), code=requests.codes.moved)
            headers = response.headers
            headers['Retry-After'] = RETRY_AFTER_INTERVAL
            return response

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
                scheme=_replica.storage_schema,
                netloc=_replica.checkout_bucket,
                path="{}/{}".format(
                    get_dst_bundle_prefix(uuid, bundle_metadata[BundleMetadata.VERSION]),
                    file[BundleFileMetadata.NAME],
                ),
            ))
        elif presignedurls:
            handle = Config.get_blobstore_handle(_replica)
            file_version['url'] = handle.generate_presigned_GET_url(
                _replica.checkout_bucket,
                "{}/{}".format(
                    get_dst_bundle_prefix(uuid, bundle_metadata[BundleMetadata.VERSION]),
                    file[BundleFileMetadata.NAME],
                ),
            )
        filesresponse.append(file_version)

    return dict(
        bundle=dict(
            uuid=uuid,
            version=bundle_metadata[BundleMetadata.VERSION],
            files=filesresponse,
            creator_uid=bundle_metadata[BundleMetadata.CREATOR_UID],
        )
    )


@dss_handler
def list_versions(uuid: str):
    return ["2014-10-23T00:35:14.800221Z"]


@dss_handler
def post():
    pass


@dss_handler
def put(uuid: str, replica: str, json_request_body: dict, version: str):
    uuid = uuid.lower()
    # convert it to date-time so we can format exactly as the system requires (with microsecond precision)
    try:
        timestamp = iso8601.parse_date(version)
    except iso8601.ParseError:
        raise DSSException(
            requests.codes.bad_request,
            "illegal_version",
            f"version should be an rfc3339-compliant timestamp")
    version = datetime_to_version_format(timestamp)

    handle = Config.get_blobstore_handle(Replica[replica])
    bucket = Replica[replica].bucket

    # what's the target object name for the bundle manifest?
    bundle_manifest_key = BundleFQID(uuid=uuid, version=version).to_key()

    # decode the list of files.
    files = list()
    filenames: typing.Set[str] = set()
    for file in json_request_body['files']:
        name = file['name']
        if name in filenames:
            raise DSSException(
                requests.codes.bad_request,
                "duplicate_filename",
                f"Duplicate file name detected: {name}. This test fails on the first occurance. Please check bundle "
                "layout to ensure no duplicated file names are present."
            )
        filenames.add(name)
        files.append({'user_supplied_metadata': file})

    time_left = nestedcontext.inject("time_left")

    while True:  # each time through the outer while-loop, we try to gather up all the file metadata.
        for file in files:
            user_supplied_metadata = file['user_supplied_metadata']
            metadata_key = FileFQID(
                uuid=user_supplied_metadata['uuid'],
                version=user_supplied_metadata['version'],
            ).to_key()
            if 'file_metadata' not in file:
                try:
                    file_metadata = handle.get(bucket, metadata_key)
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
            requests.codes.bad_request,
            "file_missing",
            f"Could not find file {missing_file_user_metadata['uuid']}/{missing_file_user_metadata['version']}."
        )

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

    try:
        created, idempotent = _idempotent_save(
            handle,
            bucket,
            bundle_manifest_key,
            bundle_metadata,
        )
    except BlobStoreTimeoutError:
        raise DSSException(
            requests.codes.unavailable,
            "service_unavailable",
            f"Service unavailable due to unusually high load/latency"
        )

    if not idempotent:
        raise DSSException(
            requests.codes.conflict,
            "bundle_already_exists",
            f"bundle with UUID {uuid} and version {version} already exists"
        )
    status_code = requests.codes.created if created else requests.codes.ok

    return jsonify(dict(version=version)), status_code


@dss_handler
def delete(uuid: str, replica: str, json_request_body: dict, version: str=None):
    email = request.token_info['email']

    if email not in ADMIN_USER_EMAILS:
        raise DSSException(
            requests.codes.forbidden,
            "forbidden",
            f"You can't delete bundles with these credentials!",
        )

    uuid = uuid.lower()
    version = datetime_to_version_format(iso8601.parse_date(version)) if version else None

    tombstone_id = BundleTombstoneID(uuid=uuid, version=version)
    bundle_prefix = tombstone_id.to_key_prefix()
    tombstone_object_data = _create_tombstone_data(
        email=email,
        reason=json_request_body.get('reason'),
        version=version,
    )

    handle = Config.get_blobstore_handle(Replica[replica])
    bucket = Replica[replica].bucket

    if test_object_exists(handle, bucket, bundle_prefix, test_type=ObjectTest.PREFIX):
        created, idempotent = _idempotent_save(
            handle,
            bucket,
            tombstone_id.to_key(),
            tombstone_object_data
        )
        if not idempotent:
            raise DSSException(
                requests.codes.conflict,
                f"bundle_tombstone_already_exists",
                f"bundle tombstone with UUID {uuid} and version {version} already exists",
            )
        status_code = requests.codes.ok
        response_body = dict()  # type: dict
    else:
        status_code = requests.codes.not_found
        response_body = dict(title="bundle not found")

    return jsonify(response_body), status_code


def _idempotent_save(blobstore: BlobStore, bucket: str, key: str, data: dict) -> typing.Tuple[bool, bool]:
    """
    _idempotent_save attempts to save an object to the BlobStore. Its return values indicate whether the save was made
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
        blobstore.upload_file_handle(
            bucket,
            key,
            io.BytesIO(json.dumps(data).encode("utf-8")),
        )
        return True, True


def _create_tombstone_data(email: str, reason: str, version: typing.Optional[str]) -> dict:
    # Future-proofing the case in which garbage collection is added
    data = dict(
        email=email,
        reason=reason,
        admin_deleted=True,
    )
    # optional params
    if version is not None:
        data.update(version=version)
    return data

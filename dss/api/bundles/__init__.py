import io
import os
import json
import time
import typing
import datetime

import nestedcontext
import requests
from cloud_blobstore import BlobNotFoundError, BlobStore, BlobStoreTimeoutError
from flask import jsonify, redirect, request, make_response
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice


from dss import DSSException, dss_handler, DSSForbiddenException
from dss.api.search import PerPageBounds
from dss.config import Config, Replica
from dss.storage.blobstore import idempotent_save, test_object_exists, ObjectTest
from dss.storage.bundles import get_bundle_manifest, save_bundle_manifest
from dss.storage.checkout import CheckoutError, TokenError
from dss.storage.checkout.bundle import get_dst_bundle_prefix, verify_checkout
from dss.storage.identifiers import BundleTombstoneID, BundleFQID, FileFQID, TOMBSTONE_SUFFIX
from dss.storage.hcablobstore import BundleFileMetadata, BundleMetadata, FileMetadata
from dss.util import UrlBuilder, security, hashabledict, multipart_parallel_upload
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
        per_page: int,
        version: str = None,
        directurls: bool = False,
        presignedurls: bool = False,
        token: str = None,
        start_at: int = 0,
):
    if directurls and presignedurls:
        raise DSSException(
            requests.codes.bad_request, "only_one_urltype", "only enable one of `directurls` or `presignedurls`")

    _replica = Replica[replica]
    bundle_metadata = get_bundle_manifest(uuid, _replica, version)
    if bundle_metadata is None:
        raise DSSException(404, "not_found", "Cannot find bundle!")
    if version is None:
        version = bundle_metadata[BundleMetadata.VERSION]

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

    all_files = bundle_metadata[BundleMetadata.FILES]

    link = None
    if len(all_files) - start_at > per_page:
        next_url = UrlBuilder(request.url)
        next_url.replace_query("start_at", str(start_at + per_page))
        next_url.replace_query("version", version)
        next_url.replace_query("token", token)
        link = f"<{next_url}>; rel='next'"

    files = all_files[start_at:start_at + per_page]

    filesresponse = []  # type: typing.List[dict]
    for _file in files:
        file_version = {
            'name': _file[BundleFileMetadata.NAME],
            'content-type': _file[BundleFileMetadata.CONTENT_TYPE],
            'size': _file[BundleFileMetadata.SIZE],
            'uuid': _file[BundleFileMetadata.UUID],
            'version': _file[BundleFileMetadata.VERSION],
            'crc32c': _file[BundleFileMetadata.CRC32C],
            's3_etag': _file[BundleFileMetadata.S3_ETAG],
            'sha1': _file[BundleFileMetadata.SHA1],
            'sha256': _file[BundleFileMetadata.SHA256],
            'indexed': _file[BundleFileMetadata.INDEXED],
        }
        if directurls:
            file_version['url'] = str(UrlBuilder().set(
                scheme=_replica.storage_schema,
                netloc=_replica.checkout_bucket,
                path="{}/{}".format(
                    get_dst_bundle_prefix(uuid, bundle_metadata[BundleMetadata.VERSION]),
                    _file[BundleFileMetadata.NAME],
                ),
            ))
        elif presignedurls:
            handle = Config.get_blobstore_handle(_replica)
            file_version['url'] = handle.generate_presigned_GET_url(
                _replica.checkout_bucket,
                "{}/{}".format(
                    get_dst_bundle_prefix(uuid, bundle_metadata[BundleMetadata.VERSION]),
                    _file[BundleFileMetadata.NAME],
                ),
            )
        filesresponse.append(file_version)

    response_body = dict(
        bundle=dict(
            uuid=uuid,
            version=bundle_metadata[BundleMetadata.VERSION],
            files=filesresponse,
            creator_uid=bundle_metadata[BundleMetadata.CREATOR_UID],
        )
    )

    if link is None:
        return response_body
    else:
        response = make_response(jsonify(response_body), requests.codes.partial)
        response.headers['Link'] = link
        return response

@dss_handler
def enumerate(replica: str, prefix: str, token: str = None,
              per_page: int = PerPageBounds.per_page_max,
              search_after: typing.Optional[str] = None):

    storage_handler = Config.get_blobstore_handle(Replica[replica])
    key_prefix = prefix if prefix else 'bundles/'  # might need to add filtering over here (no files/blobs allowed)
    """Does this even need to have prefix? the only option is bundles, only in the checkout bucket does the
        Bundles/ prefix actually have files, and thats after a --presigned url, otherwise contents after bundles/ are not
        exposed........"""
    kwargs = dict(bucket=Replica[replica].bucket, prefix=key_prefix, k_page_max=per_page)
    if search_after:
        kwargs['start_after_key'] = search_after
    if token:
        kwargs['token'] = token
    prefix_iterator = storage_handler.list_v2(**kwargs)

    keys = [x[0] for x in islice(prefix_iterator, 0, per_page) if not x.endswith(TOMBSTONE_SUFFIX)]
    try:
        prefix_iterator.next()
    except StopIteration:
        """ No more keys, we have them all
        # This addresses issue of having 1000 bundles, asking for 500, then another 500.
        # if this was not used then there would be another partial return, which would cause errors. 
        # 206 ->206 -> ? vs 206 -> 200 
        # Delete this block of text 
        """
        return make_response(jsonify(dict(keys=keys)), requests.codes.ok)
    if len(keys) < per_page:
        response = make_response(jsonify(dict(keys=keys)), requests.codes.ok)
    else:
        payload = dict(keys=keys, token=prefix_iterator.token, search_after=keys[-1])
        response = make_response(jsonify(payload), requests.codes.partial)
        # TODO pass back LINK in the headers for the user to follow. if so; token and search after can be removed.

    return response



    # per-page reponse, give back incomplete headers


@dss_handler
@security.authorized_group_required(['hca'])
def put(uuid: str, replica: str, json_request_body: dict, version: str):
    uuid = uuid.lower()

    files = build_bundle_file_metadata(Replica[replica], json_request_body['files'])
    detect_filename_collisions(files)

    # build a manifest consisting of all the files.
    bundle_metadata = {
        BundleMetadata.FORMAT: BundleMetadata.FILE_FORMAT_VERSION,
        BundleMetadata.VERSION: version,
        BundleMetadata.FILES: files,
        BundleMetadata.CREATOR_UID: json_request_body['creator_uid'],
    }

    status_code = _save_bundle(Replica[replica], uuid, version, bundle_metadata)

    return jsonify(dict(version=bundle_metadata['version'], manifest=bundle_metadata)), status_code


def _save_bundle(replica: Replica, uuid: str, version: str, bundle_metadata: dict) -> int:
    try:
        created, idempotent = save_bundle_manifest(replica, uuid, version, bundle_metadata)
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

    return status_code


def bundle_file_id_metadata(bundle_file_metadata):
    return hashabledict({
        'name': bundle_file_metadata['name'],
        'uuid': bundle_file_metadata['uuid'],
        'version': bundle_file_metadata['version'],
    })


@dss_handler
@security.authorized_group_required(['hca'])
def patch(uuid: str, json_request_body: dict, replica: str, version: str):
    bundle = get_bundle_manifest(uuid, Replica[replica], version)
    if bundle is None:
        raise DSSException(404, "not_found", "Could not find bundle for UUID {}".format(uuid))

    remove_files_set = {bundle_file_id_metadata(f) for f in json_request_body.get("remove_files", [])}
    bundle['files'] = [f for f in bundle['files'] if bundle_file_id_metadata(f) not in remove_files_set]
    add_files = json_request_body.get("add_files", [])
    bundle['files'].extend(build_bundle_file_metadata(Replica[replica], add_files))
    detect_filename_collisions(bundle['files'])

    timestamp = datetime.datetime.utcnow()
    new_bundle_version = datetime_to_version_format(timestamp)
    bundle['version'] = new_bundle_version
    _save_bundle(Replica[replica], uuid, new_bundle_version, bundle)
    return jsonify(dict(uuid=uuid, version=new_bundle_version)), requests.codes.ok


@dss_handler
@security.authorized_group_required(['hca'])
def delete(uuid: str, replica: str, json_request_body: dict, version: str = None):
    email = security.get_token_email(request.token_info)

    if email not in ADMIN_USER_EMAILS:
        raise DSSForbiddenException("You can't delete bundles with these credentials!")

    uuid = uuid.lower()
    tombstone_id = BundleTombstoneID(uuid=uuid, version=version)
    bundle_prefix = tombstone_id.to_key_prefix()
    tombstone_object_data = _create_tombstone_data(
        email=email,
        reason=json_request_body.get('reason'),
        version=version,
    )

    handle = Config.get_blobstore_handle(Replica[replica])
    if test_object_exists(handle, Replica[replica].bucket, bundle_prefix, test_type=ObjectTest.PREFIX):
        created, idempotent = idempotent_save(
            handle,
            Replica[replica].bucket,
            tombstone_id.to_key(),
            json.dumps(tombstone_object_data).encode("utf-8")
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


def build_bundle_file_metadata(replica: Replica, user_supplied_files: dict):
    handle = Config.get_blobstore_handle(replica)

    time_left = nestedcontext.inject("time_left")

    # decode the list of files.
    files = [{'user_supplied_metadata': _file} for _file in user_supplied_files]

    def _get_file_metadata(_file):
        metadata_key = FileFQID(
            uuid=_file['user_supplied_metadata']['uuid'],
            version=_file['user_supplied_metadata']['version'],
        ).to_key()
        while True:
            try:
                file_metadata = handle.get(replica.bucket, metadata_key)
            except BlobNotFoundError:
                if time_left() > PUT_TIME_ALLOWANCE_SECONDS:
                    time.sleep(1)
                else:
                    break
            else:
                return json.loads(file_metadata)
        return None

    # TODO: Consider scaling parallelization with Lambda size
    with ThreadPoolExecutor(max_workers=20) as e:
        futures = {e.submit(_get_file_metadata, _file): _file
                   for _file in files}
        for future in as_completed(futures):
            _file = futures[future]
            res = future.result()
            if res is not None:
                _file['file_metadata'] = res
            else:
                missing_file_user_metadata = _file['user_supplied_metadata']
                raise DSSException(
                    requests.codes.bad_request,
                    "file_missing",
                    f"Could not find file {missing_file_user_metadata['uuid']}/{missing_file_user_metadata['version']}."
                )

    return [
        {
            BundleFileMetadata.NAME: _file['user_supplied_metadata']['name'],
            BundleFileMetadata.UUID: _file['user_supplied_metadata']['uuid'],
            BundleFileMetadata.VERSION: _file['user_supplied_metadata']['version'],
            BundleFileMetadata.CONTENT_TYPE: _file['file_metadata'][FileMetadata.CONTENT_TYPE],
            BundleFileMetadata.SIZE: _file['file_metadata'][FileMetadata.SIZE],
            BundleFileMetadata.INDEXED: _file['user_supplied_metadata']['indexed'],
            BundleFileMetadata.CRC32C: _file['file_metadata'][FileMetadata.CRC32C],
            BundleFileMetadata.S3_ETAG: _file['file_metadata'][FileMetadata.S3_ETAG],
            BundleFileMetadata.SHA1: _file['file_metadata'][FileMetadata.SHA1],
            BundleFileMetadata.SHA256: _file['file_metadata'][FileMetadata.SHA256],
        }
        for _file in files
    ]


def detect_filename_collisions(bundle_file_metadata):
    filenames: typing.Set[str] = set()
    for _file in bundle_file_metadata:
        name = _file[BundleFileMetadata.NAME]
        if name not in filenames:
            filenames.add(name)
        else:
            raise DSSException(
                requests.codes.bad_request,
                "duplicate_filename",
                f"Duplicate file name detected: {name}. This test fails on the first occurance. Please check bundle "
                "layout to ensure no duplicated file names are present."
            )


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

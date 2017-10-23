import datetime
import io
import json
import typing

import iso8601
import requests
from cloud_blobstore import BlobNotFoundError
from flask import jsonify, make_response

from .. import DSSException, dss_handler
from ..config import Config
from ..hcablobstore import BundleFileMetadata, BundleMetadata, FileMetadata
from ..util import UrlBuilder


@dss_handler
def get(
        uuid: str,
        replica: str,
        version: str=None,
        # TODO: (ttung) once we can run the endpoints from each cloud, we should default to the local cloud.
        directurls: bool=False):
    uuid = uuid.lower()

    handle, hca_handle, bucket = Config.get_cloud_specific_handles(replica)

    if version is None:
        # list the files and find the one that is the most recent.
        prefix = "bundles/{}.".format(uuid)
        for matching_file in handle.list(bucket, prefix):
            matching_file = matching_file[len(prefix):]
            if version is None or matching_file > version:
                version = matching_file

    if version is None:
        # no matches!
        raise DSSException(404, "not_found", "Cannot find file!")

    # retrieve the bundle metadata.
    try:
        bundle_metadata = json.loads(
            handle.get(
                bucket,
                "bundles/{}.{}".format(uuid, version)
            ).decode("utf-8"))
    except BlobNotFoundError as ex:
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
                path=f"blobs/{file[BundleFileMetadata.SHA256]}.{file[BundleFileMetadata.SHA1]}.{file[BundleFileMetadata.S3_ETAG]}.{file[BundleFileMetadata.CRC32C]}"  # noqa
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


@dss_handler
def list_versions(uuid: str):
    return ["2014-10-23T00:35:14.800221Z"]


@dss_handler
def post():
    pass


@dss_handler
def put(uuid: str, replica: str, json_request_body: dict, version: str=None):
    uuid = uuid.lower()
    if version is not None:
        # convert it to date-time so we can format exactly as the system requires (with microsecond precision)
        timestamp = iso8601.parse_date(version)
    else:
        timestamp = datetime.datetime.utcnow()
    version = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

    handle, hca_handle, bucket = Config.get_cloud_specific_handles(replica)

    # what's the target object name for the bundle manifest?
    bundle_manifest_object_name = "bundles/" + uuid + "." + version

    # decode the list of files.
    files = [{'user_supplied_metadata': file}
             for file in json_request_body['files']]

    # fetch the corresponding file metadata files.  if any do not exist, immediately fail.
    for file in files:
        user_supplied_metadata = file['user_supplied_metadata']
        metadata_path = 'files/{}.{}'.format(user_supplied_metadata['uuid'], user_supplied_metadata['version'])
        try:
            file_metadata = handle.get(bucket, metadata_path)
        except BlobNotFoundError:
            raise DSSException(
                requests.codes.conflict,
                "file_missing",
                f"Could not find file {user_supplied_metadata['uuid']}/{user_supplied_metadata['version']}."
            )
        file['file_metadata'] = json.loads(file_metadata)

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

    # if it already exists, then it's a failure.
    try:
        handle.get_user_metadata(bucket, bundle_manifest_object_name)
    except BlobNotFoundError:
        # write manifest to persistent store
        handle.upload_file_handle(
            bucket,
            bundle_manifest_object_name,
            io.BytesIO(json.dumps(bundle_metadata).encode("utf-8")))
        status_code = requests.codes.created

        # TODO: write transaction to persistent store.
    else:
        # fetch the file metadata, compare it to what we have.
        existing_bundle_metadata = json.loads(
            handle.get(bucket, bundle_manifest_object_name).decode("utf-8"))

        if existing_bundle_metadata != bundle_metadata:
            raise DSSException(
                requests.codes.conflict,
                "bundle_already_exists",
                f"bundle with UUID {uuid} and version {version} already exists")
        status_code = requests.codes.ok

    return jsonify(dict(version=version)), status_code

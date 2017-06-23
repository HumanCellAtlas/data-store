import datetime
import io
import json
import uuid

import pyrfc3339
import requests
from flask import jsonify, make_response, request

from ..blobstore import BlobNotFoundError
from ..config import Config
from ..hcablobstore import BundleFileMetadata, BundleMetadata, FileMetadata


def get(
        uuid: str,
        version: str=None,
        # TODO: (ttung) once we can run the endpoints from each cloud, we should default to the local cloud.
        replica: str="AWS"):
    uuid = uuid.lower()

    handle, hca_handle, bucket = \
        Config.get_cloud_specific_handles(replica)

    if version is None:
        # list the files and find the one that is the most recent.
        prefix = "bundles/{}.".format(uuid)
        for matching_file in handle.list(bucket, prefix):
            matching_file = matching_file[len(prefix):]
            if version is None or matching_file > version:
                version = matching_file

    if version is None:
        # no matches!
        return make_response("Cannot find file!", 404)

    # retrieve the bundle metadata.
    try:
        bundle_metadata = json.loads(
            handle.get(
                bucket,
                "bundles/{}.{}".format(uuid, version)
            ).decode("utf-8"))
    except BlobNotFoundError as ex:
        return jsonify(dict(
            message="Cannot find bundle.",
            exception=str(ex),
            HTTPStatusCode=requests.codes.not_found)), requests.codes.not_found

    return dict(
        bundle=dict(
            uuid=uuid,
            version=version,
            files=[
                {
                    'name': file[BundleFileMetadata.NAME],
                    'content-type': file[BundleFileMetadata.CONTENT_TYPE],
                    'uuid': file[BundleFileMetadata.UUID],
                    'version': file[BundleFileMetadata.VERSION],
                    'crc32c': file[BundleFileMetadata.CRC32C],
                    's3_etag': file[BundleFileMetadata.S3_ETAG],
                    'sha1': file[BundleFileMetadata.SHA1],
                    'sha256': file[BundleFileMetadata.SHA256],
                }
                for file in bundle_metadata[BundleMetadata.FILES]
            ],
            creator_uid=bundle_metadata[BundleMetadata.CREATOR_UID],
        )
    )


def list_versions(uuid: str):
    return ["2014-10-23T00:35:14.800221Z"]


def list():
    return dict(bundles=[dict(uuid=str(uuid.uuid4()), versions=[])])


def post():
    pass


def put(uuid: str, replica: str, version: str=None):
    uuid = uuid.lower()
    if version is not None:
        # convert it to date-time so we can format exactly as the system requires (with microsecond precision)
        timestamp = pyrfc3339.parse(version, utc=True)
    else:
        timestamp = datetime.datetime.utcnow()
    version = pyrfc3339.generate(
        timestamp,
        accept_naive=True,
        microseconds=True,
        utc=True)

    handle, hca_handle, bucket = \
        Config.get_cloud_specific_handles("aws")

    # what's the target object name for the bundle manifest?
    bundle_manifest_object_name = "bundles/" + uuid + "." + version

    # if it already exists, then it's a failure.
    try:
        handle.get_metadata(bucket, bundle_manifest_object_name)
    except BlobNotFoundError:
        pass
    else:
        # TODO: (ttung) better error messages pls.
        return (
            make_response("bundle already exists!"),
            requests.codes.conflict
        )

    # decode the list of files.
    request_data = request.get_json()
    files = [{'user_supplied_metadata': file}
             for file in request_data['files']]

    # fetch the corresponding file metadata files.  if any do not exist, immediately fail.
    for file in files:
        user_supplied_metadata = file['user_supplied_metadata']
        metadata_path = 'files/{}.{}'.format(user_supplied_metadata['uuid'], user_supplied_metadata['version'])
        file['file_metadata'] = json.loads(handle.get(bucket, metadata_path))

    # build a manifest consisting of all the files.
    document = json.dumps({
        BundleMetadata.FORMAT: BundleMetadata.FILE_FORMAT_VERSION,
        BundleMetadata.VERSION: version,
        BundleMetadata.FILES: [
            {
                BundleFileMetadata.NAME: file['user_supplied_metadata']['name'],
                BundleFileMetadata.UUID: file['user_supplied_metadata']['uuid'],
                BundleFileMetadata.VERSION: file['user_supplied_metadata']['version'],
                BundleFileMetadata.CONTENT_TYPE: file['file_metadata'][FileMetadata.CONTENT_TYPE],
                BundleFileMetadata.INDEXED: file['user_supplied_metadata']['indexed'],
                BundleFileMetadata.CRC32C: file['file_metadata'][FileMetadata.CRC32C],
                BundleFileMetadata.S3_ETAG: file['file_metadata'][FileMetadata.S3_ETAG],
                BundleFileMetadata.SHA1: file['file_metadata'][FileMetadata.SHA1],
                BundleFileMetadata.SHA256: file['file_metadata'][FileMetadata.SHA256],
            }
            for file in files
        ],
        BundleMetadata.CREATOR_UID: request_data['creator_uid']
    })

    # write manifest to persistent store
    handle.upload_file_handle(
        bucket,
        bundle_manifest_object_name,
        io.BytesIO(document.encode("utf-8")))

    # TODO: write transaction to persistent store.

    return jsonify(dict(version=version)), requests.codes.created

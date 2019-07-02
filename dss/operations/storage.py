"""
Storage consistency checks: verify and repair storage integrity within a single replica.
"""
import json
import typing
import logging
import argparse
import math
from uuid import uuid4
from traceback import format_exc

import boto3
from cloud_blobstore import BlobNotFoundError
from dcplib.aws.sqs import SQSMessenger
from dcplib.s3_multipart import get_s3_multipart_chunk_size

from dss import Config, Replica
from dss.storage.hcablobstore import compose_blob_key
from dss.operations import dispatch
from dss.operations.util import command_queue_url, map_bucket
from dss.events.handlers.sync import dependencies_exist
from dss.storage.identifiers import BUNDLE_PREFIX, FILE_PREFIX, COLLECTION_PREFIX
from dss.util.aws import resources


logger = logging.getLogger(__name__)


class StorageOperationHandler:
    def __init__(self, argv: typing.List[str], args: argparse.Namespace):
        self.keys = args.keys.copy() if args.keys else None
        self.entity_type = args.entity_type
        self.job_id = args.job_id
        self.replica = Replica[args.replica]
        self.handle = Config.get_blobstore_handle(self.replica)

    def forward_command_to_lambda(self, argv: typing.List[str], args: argparse.Namespace):
        """
        This transforms a command into a format appropriate for Lambda execution. To take advantage of Lambda scaling,
        commands operating on multiple keys are forwarded as multiple commands operating on a single key.
        """
        target, action = argv[0:2]
        job_id = self.job_id or uuid4()
        cmd_template = f"{target} {action} --job-id {job_id} --replica {self.replica.name} --keys {{}}"

        # dump forwarded command format to stdout, including correlation id
        print(f"Forwarding `{cmd_template}`")

        def forward_keys(keys):
            with SQSMessenger(command_queue_url) as sqsm:
                for key in keys:
                    sqsm.send(cmd_template.format(key))

        if args.keys is not None:
            forward_keys(args.keys)
        else:
            map_bucket(forward_keys, self.handle, self.replica.bucket, f"{self.entity_type}/")

    def process_command_locally(self, argv: typing.List[str], args: argparse.Namespace):
        if self.keys is not None:
            for key in self.keys:
                self.process_key(key)
        else:
            def process_keys(keys):
                for key in keys:
                    self.process_key(key)

            map_bucket(process_keys, self.handle, self.replica.bucket, f"{self.entity_type}/")

    def log_warning(self, name: str, info: dict):
        logger.warning(json.dumps({'job_id': self.job_id, name: info}))

    def log_error(self, name: str, info: dict):
        logger.error(json.dumps({'job_id': self.job_id, name: info}))

    def process_key(self, key):
        raise NotImplementedError()

    def __call__(self, argv: typing.List[str], args: argparse.Namespace):
        if args.forward_to_lambda:
            self.forward_command_to_lambda(argv, args)
        else:
            self.process_command_locally(argv, args)

storage = dispatch.target(
    "storage",
    arguments={"--forward-to-lambda": dict(default=False,
                                           action="store_true",
                                           help=('execute this command with Lambda parallelization\n'
                                                 'output will be available in CloudWatch logs')),
               "--replica": dict(choices=[r.name for r in Replica], required=True),
               "--entity-type": dict(choices=[FILE_PREFIX, BUNDLE_PREFIX, COLLECTION_PREFIX]),
               "--job-id": dict(default=None),
               "--keys": dict(default=None, nargs="*", help="keys to check. Omit to check all files")},
    help=__doc__
)

@storage.action("verify-file-blob-metadata",
                arguments={"--entity-type": dict(default=FILE_PREFIX, choices=[FILE_PREFIX])})
class verify_file_blob_metadata(StorageOperationHandler):
    """
    Verify that:
    1) file size matches blob size
    2) file content-type matches blob content-type
    3) TODO: content-disposition is _not_ set for blob

    Local execution examples:
    scripts/dss-ops.py storage verify-file-blob-metadata --replica $rplc --keys $key1 $key2
    scripts/dss-ops.py storage verify-file-blob-metadata --replica $rplc

    Lambda execution examples (output will be dumped to CloudWatch logs):
    scripts/dss-ops.py storage verify-file-blob-metadata --replica $rplc --forward-to-lambda
    scripts/dss-ops.py storage verify-file-blob-metadata --replica $rplc --keys $key1 $key2 --forward-to-lambda
    """
    def process_key(self, key):
        file_metadata = json.loads(self.handle.get(self.replica.bucket, key))
        blob_key = compose_blob_key(file_metadata)
        try:
            blob_size = self.handle.get_size(self.replica.bucket, blob_key)
            blob_etag = self.handle.get_cloud_checksum(self.replica.bucket, blob_key)
        except BlobNotFoundError:
            self.log_warning(BlobNotFoundError.__name__, dict(key=key, replica=self.replica.name, blob_key=blob_key))
        else:
            blob_content_type = self.handle.get_content_type(self.replica.bucket, blob_key)
            if file_metadata['size'] != blob_size:
                self.log_warning("FileSizeMismatch",
                                 dict(key=key,
                                      replica=self.replica.name,
                                      file_metadata_size=file_metadata['size'],
                                      blob_size=blob_size))
            if file_metadata['content-type'] != blob_content_type:
                self.log_warning("FileContentTypeMismatch",
                                 dict(key=key,
                                      replica=self.replica.name,
                                      file_metadata_content_type=file_metadata['content-type'],
                                      blob_content_type=blob_content_type))
            if file_metadata['s3-etag'] != blob_etag:
                self.log_warning("FileEtagMismatch",
                                 dict(key=key,
                                      replica=self.replica.name,
                                      file_etag=file_metadata['s3-etag'],
                                      blob_etag=blob_etag))

@storage.action("repair-file-blob-metadata",
                arguments={"--entity-type": dict(default="file", choices=["file"])})
class repair_file_blob_metadata(StorageOperationHandler):
    """
    Make blob metadata consistent with file metadata.
    """
    def process_key(self, key):
        try:
            file_metadata = json.loads(self.handle.get(self.replica.bucket, key))
            blob_key = compose_blob_key(file_metadata)
            blob_content_type = self.handle.get_content_type(self.replica.bucket, blob_key)
            client = Config.get_native_handle(self.replica)
            if blob_content_type != file_metadata['content-type']:
                if Replica.aws == self.replica:
                    update_aws_content_type(client, self.replica.bucket, blob_key, file_metadata['content-type'])
                elif Replica.gcp == self.replica:
                    update_gcp_content_type(client, self.replica.bucket, blob_key, file_metadata['content-type'])
            elif Replica.aws == self.replica:
                blob_etag = self.handle.get_cloud_checksum(self.replica.bucket, blob_key)
                if blob_etag != file_metadata['s3-etag']:
                    update_aws_content_type(client, self.replica.bucket, blob_key, file_metadata['content-type'])
        except BlobNotFoundError as e:
            self.log_warning("BlobNotFoundError", dict(key=key, replica=self.replica.name, error=str(e)))
        except json.decoder.JSONDecodeError as e:
            self.log_warning("JSONDecodeError", dict(key=key, replica=self.replica.name, error=str(e)))
        except Exception as e:
            self.log_error("Exception", dict(key=key, error=format_exc()))

@storage.action("verify-referential-integrity",
                mutually_exclusive=["--entity-type", "--keys"])
class verify_referential_integrity(StorageOperationHandler):
    """
    This uses DSS API patterns to verify the referential integrity of datastore objects:
    1) For files, verify that blob object exists
    2) For bundles, verify that file metadata objects exist
    3) For collections, verify that all items exist

    Local execution examples:
    scripts/dss-ops.py storage verify-referential-integrity --replica $rplc --keys $key1 $key2
    scripts/dss-ops.py storage verify-referential-integrity --replica $rplc --entity-type bundles

    Lambda execution examples (output will be dumped to CloudWatch logs):
    scripts/dss-ops.py storage verify-referential-integrity --replica $rplc --keys $key1 $key2 --forward-to-lambda
    scripts/dss-ops.py storage verify-referential-integrity --replica $rplc --entity-type bundles --forward-to-lambda
    """
    def process_key(self, key):
        logger.debug("%s Checking %s %s", self.job_id, key, self.replica)
        if not dependencies_exist(self.replica, self.replica, key):
            self.log_warning("EntityMissingDependencies", dict(key=key, replica=self.replica.name))

# TODO: Move to cloud_blobstore
def update_aws_content_type(s3_client, bucket, key, content_type):
    blob = resources.s3.Bucket(bucket).Object(key)
    size = blob.content_length
    source_etag = blob.e_tag
    part_size = get_s3_multipart_chunk_size(size)
    if size <= part_size:
        s3_client.copy_object(Bucket=bucket,
                              Key=key,
                              CopySource=dict(Bucket=bucket, Key=key),
                              ContentType=content_type,
                              MetadataDirective="REPLACE")
    else:
        blobstore = Config.get_blobstore_handle(Replica.aws)
        resp = s3_client.create_multipart_upload(Bucket=bucket,
                                                 Key=key,
                                                 Metadata=blobstore.get_user_metadata(bucket, key),
                                                 ContentType=content_type)
        upload_id = resp['UploadId']
        multipart_upload = dict(Parts=list())
        for i in range(math.ceil(size / part_size)):
            start_range = i * part_size
            end_range = (i + 1) * part_size - 1
            if end_range >= size:
                end_range = size - 1
            resp = s3_client.upload_part_copy(CopySource=dict(Bucket=bucket, Key=key),
                                              Bucket=bucket,
                                              Key=key,
                                              CopySourceRange=f"bytes={start_range}-{end_range}",
                                              PartNumber=i + 1,
                                              UploadId=upload_id)
            multipart_upload['Parts'].append(dict(ETag=resp['CopyPartResult']['ETag'], PartNumber=i + 1))
        s3_client.complete_multipart_upload(Bucket=bucket,
                                            Key=key,
                                            MultipartUpload=multipart_upload,
                                            UploadId=upload_id)

# TODO: Move to cloud_blobstore
def update_gcp_content_type(gs_client, bucket, key, content_type):
    gs = Config.get_native_handle(Replica.gcp)
    gs_bucket = gs.bucket(bucket)
    gs_blob = gs_bucket.blob(key)
    gs_blob.reload()
    gs_blob.content_type = content_type
    gs_blob.patch()

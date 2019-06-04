"""
Storage consistency checks: verify and repair storage integrity within a single replica.
"""
import json
import typing
import logging
import argparse
from uuid import uuid4

from cloud_blobstore import BlobNotFoundError
from dcplib.aws.sqs import SQSMessenger

from dss import Config, Replica
from dss.storage.hcablobstore import compose_blob_key
from dss.operations import dispatch
from dss.operations.util import command_queue_url, map_bucket
from dss.events.handlers.sync import dependencies_exist
from dss.storage.identifiers import BUNDLE_PREFIX, FILE_PREFIX, COLLECTION_PREFIX


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
        cmd_template = f"{argv[0]} {argv[1]}"
        args.job_id = args.job_id or uuid4()
        if "entity_type" in args.__dict__.keys() and "keys" in args.__dict__.keys():
            del args.entity_type
        for argname, argval in args.__dict__.items():
            argname = argname.replace("_", "-")
            if argname not in ["forward-to-lambda", "keys", "func"]:
                cmd_template += f" --{argname} {argval}"
        cmd_template += " --keys {}"

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
        except BlobNotFoundError:
            self.log_warning(BlobNotFoundError.__name__, dict(key=key, blob_key=blob_key))
        else:
            blob_content_type = self.handle.get_content_type(self.replica.bucket, blob_key)
            if file_metadata['size'] != blob_size:
                self.log_warning("FileSizeMismatch",
                                 dict(key=key,
                                      file_metadata_size=file_metadata['size'],
                                      blob_size=blob_size))
            if file_metadata['content-type'] != blob_content_type:
                self.log_warning("FileContentTypeMismatch",
                                 dict(key=key,
                                      file_metadata_content_type=file_metadata['content-type'],
                                      blob_content_type=blob_content_type))

@storage.action("repair-file-blob-metadata",
                arguments={"--entity-type": dict(default="file", choices=["file"])})
class repair_blob_content_type(StorageOperationHandler):
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
        except BlobNotFoundError as e:
            self.log_warning("BlobNotFoundError", dict(key=key, error=str(e)))
        except json.decoder.JSONDecodeError as e:
            self.log_warning("JSONDecodeError", dict(key=key, error=str(e)))
        except Exception as e:
            self.log_error("Exception", dict(key=key, error=str(e)))

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
            self.log_warning("EntityMissingDependencies", dict(key=key))

# TODO: Move to cloud_blobstore
def update_aws_content_type(s3_client, bucket, key, content_type):
    s3_client.copy_object(
        Bucket=bucket,
        Key=key,
        CopySource=dict(Bucket=bucket, Key=key),
        ContentType=content_type,
        MetadataDirective="REPLACE",
    )

# TODO: Move to cloud_blobstore
def update_gcp_content_type(gs_client, bucket, key, content_type):
    gs = Config.get_native_handle(Replica.gcp)
    gs_bucket = gs.bucket(bucket)
    gs_blob = gs_bucket.blob(key)
    gs_blob.reload()
    gs_blob.content_type = content_type
    gs_blob.patch()

"""
Storage consistency checks
"""
import json
import logging
from uuid import uuid4

from cloud_blobstore import BlobNotFoundError

from dss import Config, Replica
from dss.storage.hcablobstore import compose_blob_key
from dss.operations import dispatch
from dss.operations.util import CommandForwarder, map_bucket
from dss.events.handlers.sync import dependencies_exist


logger = logging.getLogger(__name__)


class StorageOperationHandler:
    def __init__(self, argv, args):
        self.keys = args.keys.copy() if args.keys else None
        self.entity_type = args.entity_type
        self.job_id = args.job_id
        if args.replica:
            self.replica = Replica[args.replica]
            self.handle = Config.get_blobstore_handle(self.replica)

    def forward_command_to_lambda(self, argv, args):
        cmd_template = f"{argv[0]} {argv[1]}"
        if "entity_type" in args.__dict__.keys() and "keys" in args.__dict__.keys():
            del args.entity_type
        for argname, argval in args.__dict__.items():
            argname = argname.replace("_", "-")
            if argname not in ["forward-to-lambda", "keys", "func"]:
                cmd_template += f" --{argname} {argval}"
        assert "{}" not in cmd_template
        cmd_template += " --keys {}"

        def forward_keys(keys):
            with CommandForwarder() as f:
                for key in keys:
                    f.forward(cmd_template.format(key))

        if args.keys is not None:
            forward_keys(args.keys)
        else:
            map_bucket(forward_keys, self.handle, self.replica.bucket, f"{self.entity_type}s/")

    def process_command_locally(self, argv, args):
        if self.keys is not None:
            for key in self.keys:
                self.process_key(key)
        else:
            def process_keys(keys):
                for key in keys:
                    self.process_key(key)

            map_bucket(process_keys, self.handle, self.replica.bucket, f"{self.entity_type}s/")

    def log_warning(self, name: str, info: dict):
        logger.warning(json.dumps({'job_id': self.job_id, name: info}))

    def process_key(self, key):
        raise NotImplementedError()

    def __call__(self, argv, args):
        if args.forward_to_lambda:
            self.forward_command_to_lambda(argv, args)
        else:
            self.process_command_locally(argv, args)

storage = dispatch.target(
    "storage",
    arguments={"--forward-to-lambda": dict(default=False, action="store_true"),
               "--replica": dict(choices=[r.name for r in Replica], required=True),
               "--entity-type": dict(choices=["file", "bundle", "collection"]),
               "--keys": dict(default=None, nargs="*", help="keys to check. Omit to check all files")},
    help=__doc__
)

@storage.action("verify-file-blob-metadata",
                arguments={"--entity-type": dict(default="file", choices=["file"])})
class verify_file_blob_metadata(StorageOperationHandler):
    """
    Verify that:
        1) file size matches blob size
        2) file content-type matches blob content-type
        3) TODO: content-disposition is _not_ set for blob
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

@storage.action("verify-referential-integrity",
                mutually_exclusive=["--entity-type", "--keys"])
class verify_referential_integrity(StorageOperationHandler):
    """
    This uses DSS API patterns to verify the referential integrity of datastore objects:
        1) For files, verify that blob object exists
        2) For bundles, verify that file metadata objects exist
        3) For collections, verify that all items exist
    """
    def process_key(self, key):
        logger.debug("%s Checking %s %s", self.job_id, key, self.replica)
        if not dependencies_exist(self.replica, self.replica, key):
            self.log_warning("EnttyMissingDependencies", dict(key=key))

import typing

from chainedawslambda.s3copyclient import S3CopyTask, S3CopyTaskKeys

from ...api import files
from ...blobstore.s3 import S3BlobStore

# this must match the lambda name in daemons/Makefile
AWS_S3_COPY_AND_WRITE_METADATA_CLIENT_PREFIX = "dss-s3-copy-write-metadata-"

class S3CopyWriteBundleTaskKeys(S3CopyTaskKeys):
    METADATA = "metadata"
    FILE_UUID = "file_uuid"
    FILE_VERSION = "file_version"


class S3CopyWriteBundleTask(S3CopyTask):
    """
    This is a chunked task that does a multipart copy from one blob to another and writes a bundle manifest.
    """
    def __init__(self, state: dict, *args, **kwargs) -> None:
        super().__init__(state)
        self.metadata = state[S3CopyWriteBundleTaskKeys.METADATA]
        self.file_uuid = state[S3CopyWriteBundleTaskKeys.FILE_UUID]
        self.file_version = state[S3CopyWriteBundleTaskKeys.FILE_VERSION]

    def get_state(self) -> dict:
        state = super().get_state()
        state[S3CopyWriteBundleTaskKeys.METADATA] = self.metadata
        state[S3CopyWriteBundleTaskKeys.FILE_UUID] = self.file_uuid
        state[S3CopyWriteBundleTaskKeys.FILE_VERSION] = self.file_version
        return state

    @property
    def expected_max_one_unit_runtime_millis(self) -> int:
        # expect that in the worst case, we take 60 seconds to copy one part.
        return 60 * 1000

    def run_one_unit(self) -> typing.Optional[dict]:
        result = super().run_one_unit()
        if result is None:
            return result

        # subtask is done!  let's write the file metadata.
        handle = S3BlobStore()

        files.write_file_metadata(handle, self.destination_bucket, self.file_uuid, self.file_version, self.metadata)

        return {
            S3CopyWriteBundleTaskKeys.FILE_UUID: self.file_uuid,
            S3CopyWriteBundleTaskKeys.FILE_VERSION: self.file_version,
        }

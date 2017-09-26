import typing

from chainedawslambda import Task
from chainedawslambda.s3copyclient import S3CopyTask

from ...api import files
from ...blobstore.s3 import S3BlobStore

# this must match the lambda name in daemons/Makefile
AWS_S3_COPY_AND_WRITE_METADATA_CLIENT_PREFIX = "dss-s3-copy-write-metadata-"


class S3CopyWriteBundleTaskKeys:
    METADATA = "metadata"
    FILE_UUID = "file_uuid"
    FILE_VERSION = "file_version"
    STAGE = "stage"
    COPY_STATE = "copy_state"

    class S3CopyWriteBundleTaskStages:
        COPYING = "copying"
        WRITING_METADATA = "writing_metadata"


class S3CopyWriteBundleTask(Task[dict, dict]):
    """
    This is a chunked task that does a multipart copy from one blob to another and writes a bundle manifest.
    """
    def __init__(self, state: dict, *args, **kwargs) -> None:
        self.metadata = state[S3CopyWriteBundleTaskKeys.METADATA]
        self.file_uuid = state[S3CopyWriteBundleTaskKeys.FILE_UUID]
        self.file_version = state[S3CopyWriteBundleTaskKeys.FILE_VERSION]
        self.stage = state.get(
            S3CopyWriteBundleTaskKeys.STAGE,
            S3CopyWriteBundleTaskKeys.S3CopyWriteBundleTaskStages.COPYING)
        if S3CopyWriteBundleTaskKeys.COPY_STATE in state:
            self.copy_task = S3CopyTask(state[S3CopyWriteBundleTaskKeys.COPY_STATE])
        else:
            self.copy_task = S3CopyTask(state)

    def get_state(self) -> dict:
        return {
            S3CopyWriteBundleTaskKeys.METADATA: self.metadata,
            S3CopyWriteBundleTaskKeys.FILE_UUID: self.file_uuid,
            S3CopyWriteBundleTaskKeys.FILE_VERSION: self.file_version,
            S3CopyWriteBundleTaskKeys.STAGE: self.stage,
            S3CopyWriteBundleTaskKeys.COPY_STATE: self.copy_task.get_state(),
        }

    @staticmethod
    def setup_copy_task(
            metadata: str,
            file_uuid: str,
            file_version: str,
            source_bucket: str, source_key: str,
            destination_bucket: str, destination_key: str,
            part_size_calculator: typing.Callable[[int], int],
    ) -> dict:
        """
        Returns the initial state for a S3CopyWriteBundleTask to copy a blob from s3://`source_bucket`/`source_key` to
        s3://`destination_bucket`/`destination_key`, and to write the bundle metadata.
        """
        return {
            S3CopyWriteBundleTaskKeys.FILE_UUID: file_uuid,
            S3CopyWriteBundleTaskKeys.FILE_VERSION: file_version,
            S3CopyWriteBundleTaskKeys.METADATA: metadata,
            S3CopyWriteBundleTaskKeys.STAGE: S3CopyWriteBundleTaskKeys.S3CopyWriteBundleTaskStages.COPYING,
            S3CopyWriteBundleTaskKeys.COPY_STATE:
                S3CopyTask.setup_copy_task(
                    source_bucket, source_key,
                    destination_bucket, destination_key,
                    part_size_calculator,
                ),
        }

    @property
    def expected_max_one_unit_runtime_millis(self) -> int:
        # expect that in the worst case, we take 60 seconds to copy one part.
        return 60 * 1000

    def run_one_unit(self) -> typing.Optional[dict]:
        if self.stage == S3CopyWriteBundleTaskKeys.S3CopyWriteBundleTaskStages.COPYING:
            result = self.copy_task.run_one_unit()
            if result is None:
                return result

            # copying is done!
            self.stage = S3CopyWriteBundleTaskKeys.S3CopyWriteBundleTaskStages.WRITING_METADATA
            return None

        if self.stage == S3CopyWriteBundleTaskKeys.S3CopyWriteBundleTaskStages.WRITING_METADATA:
            handle = S3BlobStore()

            files.write_file_metadata(
                handle,
                self.copy_task.destination_bucket,
                self.file_uuid,
                self.file_version,
                self.metadata)

            return {
                S3CopyWriteBundleTaskKeys.FILE_UUID: self.file_uuid,
                S3CopyWriteBundleTaskKeys.FILE_VERSION: self.file_version,
            }

        raise ValueError("unknown state")

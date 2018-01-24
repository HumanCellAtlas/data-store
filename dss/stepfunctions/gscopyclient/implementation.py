import copy
import os
import typing

from cloud_blobstore.gs import GSBlobStore
from google.cloud.storage import Client

from ...api import files
from ...stepfunctions.lambdaexecutor import TimedThread


# Public input/output keys for the state object.
class Key:
    SOURCE_BUCKET = "src_bucket"
    SOURCE_KEY = "src_key"
    DESTINATION_BUCKET = "dst_bucket"
    DESTINATION_KEY = "dst_key"
    FINISHED = "finished"


# Internal key for the state object.
class _Key:
    SOURCE_CRC32C = "src_crc32c"
    SIZE = "size"
    TOKEN = "token"


def get_gcp_client():
    # TODO: (ttung) remove this once Config.get_cloud_specific_handles is refactored.
    credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    return Client.from_service_account_json(credentials)


def setup_copy_task(event, lambda_context):
    source_bucket = event[Key.SOURCE_BUCKET]
    source_key = event[Key.SOURCE_KEY]

    gcp_client = get_gcp_client()
    blob = gcp_client.bucket(source_bucket).get_blob(source_key)
    source_crc32c = blob.crc32c
    source_size = blob.size

    event[_Key.SOURCE_CRC32C] = source_crc32c
    event[_Key.SIZE] = source_size
    event[Key.FINISHED] = False

    return event


def copy_worker(event, lambda_context):
    class CopyWorkerTimedThread(TimedThread[dict]):
        def __init__(self, timeout_seconds: float, state: dict) -> None:
            super().__init__(timeout_seconds, state)
            self.gcp_client = get_gcp_client()

            self.source_bucket = state[Key.SOURCE_BUCKET]
            self.source_key = state[Key.SOURCE_KEY]
            self.source_crc32c = state[_Key.SOURCE_CRC32C]
            self.destination_bucket = state[Key.DESTINATION_BUCKET]
            self.destination_key = state[Key.DESTINATION_KEY]
            self.size = state[_Key.SIZE]

        def run(self) -> dict:
            state = self.get_state_copy()
            src_blob = self.gcp_client.bucket(self.source_bucket).get_blob(self.source_key)
            dst_blob = self.gcp_client.bucket(self.destination_bucket).blob(self.destination_key)

            while True:
                response = dst_blob.rewrite(src_blob, token=state.get(_Key.TOKEN, None))
                if response[0] is None:
                    state[Key.FINISHED] = True
                    return state
                else:
                    state[_Key.TOKEN] = response[0]
                    self.save_state(state)

    return CopyWorkerTimedThread(lambda_context.get_remaining_time_in_millis() / 1000, event).start()


retry_default = [
    {
        "ErrorEquals": ["States.Timeout", "States.TaskFailed"],
        "IntervalSeconds": 30,
        "MaxAttempts": 10,
        "BackoffRate": 1.618,
    },
]


sfn = {
    "StartAt": "SetupCopyTask",
    "States": {
        "SetupCopyTask": {
            "Type": "Task",
            "Resource": setup_copy_task,
            "Next": "Copy",
            "Retry": copy.deepcopy(retry_default),
        },
        "Copy": {
            "Type": "Task",
            "Resource": copy_worker,
            "Retry": copy.deepcopy(retry_default),
            "End": True,
        },
    }
}


# Public input/output keys for the copy + write-metadata state function.
class CopyWriteMetadataKey:
    FILE_UUID = "file_uuid"
    FILE_VERSION = "file_version"
    METADATA = "metadata"


def write_metadata(event, lambda_context):
    # TODO: (ttung) remove this once Config.get_cloud_specific_handles is refactored.
    credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    handle = GSBlobStore(credentials)

    destination_bucket = event[Key.DESTINATION_BUCKET]
    files.write_file_metadata(
        handle,
        destination_bucket,
        event[CopyWriteMetadataKey.FILE_UUID],
        event[CopyWriteMetadataKey.FILE_VERSION],
        event[CopyWriteMetadataKey.METADATA],
    )


copy_write_metadata_sfn = typing.cast(dict, copy.deepcopy(sfn))

# tweak to add one more state.
del copy_write_metadata_sfn['States']['Copy']['End']
copy_write_metadata_sfn['States']['Copy']['Next'] = "WriteMetadata"
copy_write_metadata_sfn['States']['WriteMetadata'] = {
    "Type": "Task",
    "Resource": write_metadata,
    "End": True,
}

import copy
import typing

from cloud_blobstore.gs import GSBlobStore

from ... import Config, Replica
from ...stepfunctions.lambdaexecutor import TimedThread


# Public input/output keys for the state object.
class Key:
    SOURCE_BUCKET = "srcbucket"
    SOURCE_KEY = "srckey"
    DESTINATION_BUCKET = "dstbucket"
    DESTINATION_KEY = "dstkey"
    FINISHED = "finished"


# Internal key for the state object.
class _Key:
    SOURCE_CRC32C = "srccrc32c"
    SIZE = "size"
    TOKEN = "token"


def setup_copy_task(event, lambda_context):
    source_bucket = event[Key.SOURCE_BUCKET]
    source_key = event[Key.SOURCE_KEY]

    gs_blobstore = typing.cast(GSBlobStore, Config.get_cloud_specific_handles(Replica.gcp)[0])
    blob = gs_blobstore.gcp_client.bucket(source_bucket).get_blob(source_key)
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
            self.gs_blobstore = typing.cast(GSBlobStore, Config.get_cloud_specific_handles(Replica.gcp)[0])

            self.source_bucket = state[Key.SOURCE_BUCKET]
            self.source_key = state[Key.SOURCE_KEY]
            self.source_crc32c = state[_Key.SOURCE_CRC32C]
            self.destination_bucket = state[Key.DESTINATION_BUCKET]
            self.destination_key = state[Key.DESTINATION_KEY]
            self.size = state[_Key.SIZE]

        def run(self) -> dict:
            state = self.get_state_copy()
            src_blob = self.gs_blobstore.gcp_client.bucket(self.source_bucket).get_blob(self.source_key)
            dst_blob = self.gs_blobstore.gcp_client.bucket(self.destination_bucket).blob(self.destination_key)

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

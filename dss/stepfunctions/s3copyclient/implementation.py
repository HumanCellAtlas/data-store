import binascii
import collections

import hashlib
import typing

import boto3
from cloud_blobstore.s3 import S3BlobStore
from dcplib.s3_multipart import get_s3_multipart_chunk_size

from dss.stepfunctions.lambdaexecutor import TimedThread
from dss.storage.files import write_file_metadata
from dss.util import parallel_worker


# CONSTANTS
LAMBDA_PARALLELIZATION_FACTOR = 32
CONCURRENT_REQUESTS = 8


# Public input/output keys for the state object.
class Key:
    SOURCE_BUCKET = "srcbucket"
    SOURCE_KEY = "srckey"
    DESTINATION_BUCKET = "dstbucket"
    DESTINATION_KEY = "dstkey"
    FINISHED = "finished"


# Internal key for the state object.
class _Key:
    SOURCE_ETAG = "srcetag"
    UPLOAD_ID = "uploadid"
    SIZE = "size"
    PART_SIZE = "partsz"
    NEXT_PART = "next"
    LAST_PART = "last"
    PART_COUNT = "count"


def setup_copy_task(event, lambda_context):
    source_bucket = event[Key.SOURCE_BUCKET]
    source_key = event[Key.SOURCE_KEY]
    destination_bucket = event[Key.DESTINATION_BUCKET]
    destination_key = event[Key.DESTINATION_KEY]

    s3_blobstore = S3BlobStore.from_environment()
    blobinfo = s3_blobstore.get_all_metadata(source_bucket, source_key)
    source_etag = blobinfo['ETag'].strip("\"")  # the ETag is returned with an extra set of quotes.
    source_size = blobinfo['ContentLength']  # type: int
    part_size = get_s3_multipart_chunk_size(source_size)
    part_count = source_size // part_size
    if part_count * part_size < source_size:
        part_count += 1
    if part_count > 1:
        mpu = s3_blobstore.s3_client.create_multipart_upload(Bucket=destination_bucket, Key=destination_key)
        event[_Key.UPLOAD_ID] = mpu['UploadId']
        event[Key.FINISHED] = False
    else:
        s3_blobstore.copy(source_bucket, source_key, destination_bucket, destination_key)
        event[_Key.UPLOAD_ID] = None
        event[Key.FINISHED] = True

    event[_Key.SOURCE_ETAG] = source_etag
    event[_Key.SIZE] = source_size
    event[_Key.PART_SIZE] = part_size
    event[_Key.PART_COUNT] = part_count

    return event


def copy_worker(event, lambda_context, slice_num):
    class CopyWorkerTimedThread(TimedThread[dict]):
        def __init__(self, timeout_seconds: float, state: dict, slice_num: int) -> None:
            super().__init__(timeout_seconds, state)
            self.slice_num = slice_num

            self.source_bucket = state[Key.SOURCE_BUCKET]
            self.source_key = state[Key.SOURCE_KEY]
            self.source_etag = state[_Key.SOURCE_ETAG]
            self.destination_bucket = state[Key.DESTINATION_BUCKET]
            self.destination_key = state[Key.DESTINATION_KEY]
            self.upload_id = state[_Key.UPLOAD_ID]
            self.size = state[_Key.SIZE]
            self.part_size = state[_Key.PART_SIZE]
            self.part_count = state[_Key.PART_COUNT]

        def run(self) -> dict:
            s3_blobstore = S3BlobStore.from_environment()
            state = self.get_state_copy()

            if _Key.NEXT_PART not in state or _Key.LAST_PART not in state:
                # missing the next/last part data.  calculate that from the branch id information.
                parts_per_branch = ((self.part_count + LAMBDA_PARALLELIZATION_FACTOR - 1) //
                                    LAMBDA_PARALLELIZATION_FACTOR)
                state[_Key.NEXT_PART] = self.slice_num * parts_per_branch + 1
                state[_Key.LAST_PART] = min(state[_Key.PART_COUNT], state[_Key.NEXT_PART] + parts_per_branch - 1)
                self.save_state(state)

            if state[_Key.NEXT_PART] > state[_Key.LAST_PART]:
                state[Key.FINISHED] = True
                return state

            queue = collections.deque(s3_blobstore.find_next_missing_parts(
                self.destination_bucket,
                self.destination_key,
                self.upload_id,
                self.part_count,
                state[_Key.NEXT_PART],
                state[_Key.LAST_PART] - state[_Key.NEXT_PART] + 1))

            if len(queue) == 0:
                state[Key.FINISHED] = True
                return state

            class ProgressReporter(parallel_worker.Reporter):
                def report_progress(inner_self, first_incomplete: int):
                    state[_Key.NEXT_PART] = first_incomplete
                    self.save_state(state)

            class CopyPartTask(parallel_worker.Task):
                def run(inner_self, subtask_id: int) -> None:
                    self.copy_one_part(subtask_id)

            runner = parallel_worker.Runner(CONCURRENT_REQUESTS, CopyPartTask, queue, ProgressReporter())
            results = runner.run()
            assert all(results)

            state[Key.FINISHED] = True
            return state

        def copy_one_part(self, part_id: int):
            byte_range = self.calculate_range_for_part(part_id)
            s3_client = boto3.client("s3")
            s3_client.upload_part_copy(
                Bucket=self.destination_bucket,
                CopySource=dict(
                    Bucket=self.source_bucket,
                    Key=self.source_key,
                ),
                CopySourceIfMatch=self.source_etag,
                CopySourceRange=f"bytes={byte_range[0]}-{byte_range[1]}",
                Key=self.destination_key,
                PartNumber=part_id,
                UploadId=self.upload_id,
            )

        def calculate_range_for_part(self, part_id) -> typing.Tuple[int, int]:
            """Calculate the byte range for `part_id`.  Assume these are S3 part IDs, which are 1-indexed."""
            start = (part_id - 1) * self.part_size
            end = part_id * self.part_size
            if end >= self.size:
                end = self.size
            end -= 1

            return start, end

    result = CopyWorkerTimedThread(lambda_context.get_remaining_time_in_millis() / 1000, event, slice_num).start()

    # because it would be comically large to have the full state for every worker, we strip the state if:
    #  1) we are finished
    #  2) we're not the 0th slice.

    if result[Key.FINISHED] and slice_num != 0:
        return {Key.FINISHED: True}
    else:
        return result


def join(event, lambda_context):
    # which parts are present?
    s3_resource = boto3.resource("s3")

    if not isinstance(event, list):
        # this is a single-part copy.
        return event

    # only the 0th worker propagates the full state.
    state = event[0]

    mpu = s3_resource.MultipartUpload(
        state[Key.DESTINATION_BUCKET], state[Key.DESTINATION_KEY], state[_Key.UPLOAD_ID])

    parts = list(mpu.parts.all())

    assert len(parts) == state[_Key.PART_COUNT]

    # it's all present!
    parts_list = [dict(ETag=part.e_tag,
                       PartNumber=part.part_number)
                  for part in parts
                  ]

    # verify that the ETag of the output file will match the source etag.
    bin_md5 = b"".join([binascii.unhexlify(part.e_tag.strip("\""))
                        for part in parts])
    composite_etag = hashlib.md5(bin_md5).hexdigest() + "-" + str(len(parts))
    assert composite_etag == state[_Key.SOURCE_ETAG]

    mpu.complete(MultipartUpload=dict(Parts=parts_list))
    return state


def _retry_default():
    return [
        {
            "ErrorEquals": ["States.Timeout", "States.TaskFailed"],
            "IntervalSeconds": 30,
            "MaxAttempts": 10,
            "BackoffRate": 1.618,
        },
    ]


def _threadpool_sfn(tid):
    return {
        "StartAt": f"TestNeeded{tid}",
        "States": {
            f"TestNeeded{tid}": {
                "Type": "Choice",
                "Choices": [{
                    "Variable": f"$.{_Key.PART_COUNT}",
                    "NumericGreaterThan": tid,
                    "Next": f"Worker{tid}"
                }],
                "Default": f"StripState{tid}",
            },
            f"Worker{tid}": {
                "Type": "Task",
                "Resource": lambda event, lambda_context: copy_worker(event, lambda_context, tid),
                "Next": f"TestFinished{tid}",
                "Retry": _retry_default(),
            },
            f"TestFinished{tid}": {
                "Type": "Choice",
                "Choices": [{
                    "Variable": "$.finished",
                    "BooleanEquals": True,
                    "Next": f"EndThread{tid}"
                }],
                "Default": f"Worker{tid}",
            },
            f"StripState{tid}": {
                "Type": "Pass",
                "Result": {Key.FINISHED: True},
                "Next": f"EndThread{tid}",
            },
            f"EndThread{tid}": {
                "Type": "Pass",
                "End": True,
            },
        }
    }


def _sfn(parallelization_factor):
    return {
        "StartAt": "SetupCopyTask",
        "States": {
            "SetupCopyTask": {
                "Type": "Task",
                "Resource": setup_copy_task,
                "Next": "ParallelChoice",
                "Retry": _retry_default(),
            },
            "ParallelChoice": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": f"$.{_Key.PART_COUNT}",
                        "NumericLessThanEquals": 1,
                        "Next": "Finalizer",
                    },
                ],
                "Default": "Threadpool",
            },
            "Threadpool": {
                "Type": "Parallel",
                "Branches": [_threadpool_sfn(tid) for tid in range(parallelization_factor)],
                "Next": "Finalizer",
                "Retry": _retry_default(),
            },
            "Finalizer": {
                "Type": "Task",
                "Resource": join,
                "End": True,
            },
        }
    }


sfn = _sfn(LAMBDA_PARALLELIZATION_FACTOR)


# Public input/output keys for the copy + write-metadata state function.
class CopyWriteMetadataKey:
    FILE_UUID = "fileuuid"
    FILE_VERSION = "fileversion"
    METADATA = "metadata"


def write_metadata(event, lambda_context):
    handle = S3BlobStore.from_environment()

    destination_bucket = event[Key.DESTINATION_BUCKET]
    write_file_metadata(
        handle,
        destination_bucket,
        event[CopyWriteMetadataKey.FILE_UUID],
        event[CopyWriteMetadataKey.FILE_VERSION],
        event[CopyWriteMetadataKey.METADATA],
    )


copy_write_metadata_sfn = _sfn(LAMBDA_PARALLELIZATION_FACTOR)

# tweak to add one more state.
del copy_write_metadata_sfn['States']['Finalizer']['End']
copy_write_metadata_sfn['States']['Finalizer']['Next'] = "WriteMetadata"
copy_write_metadata_sfn['States']['WriteMetadata'] = {
    "Type": "Task",
    "Resource": write_metadata,
    "End": True,
}

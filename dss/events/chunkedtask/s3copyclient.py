import binascii
import collections
import hashlib
import time
import typing
from concurrent.futures import ThreadPoolExecutor

import boto3

from . import Runtime, Task
from ...api import files
from ...blobstore.s3 import S3BlobStore


AWS_S3_COPY_CLIENT_NAME = "s3_copy"
AWS_S3_COPY_AND_WRITE_METADATA_CLIENT_NAME = "s3_copy_write_metadata"

AWS_S3_PARALLEL_COPY_SUPERVISOR_CLIENT_NAME = "s3_parallel_copy_supervisor"
AWS_S3_PARALLEL_COPY_WORKER_CLIENT_NAME = "s3_parallel_copy_worker"


# intuitively, this ought to be an enum, but serializing an enum is way too complicated.
class S3CopyTaskKeys:
    SOURCE_BUCKET = "srcbucket"
    SOURCE_KEY = "srckey"
    SOURCE_ETAG = "srcetag"
    DESTINATION_BUCKET = "dstbucket"
    DESTINATION_KEY = "dstkey"
    UPLOAD_ID = "uploadid"
    SIZE = "size"
    PART_SIZE = "partsz"
    NEXT_PART = "next"
    PART_COUNT = "count"


class S3CopyTask(Task[dict, typing.Any]):
    """
    This is a chunked task that does a multipart copy from one blob to another.
    """
    def __init__(self, state: dict, fetch_size: int=100, *args, **kwargs) -> None:
        self.source_bucket = state[S3CopyTaskKeys.SOURCE_BUCKET]
        self.source_key = state[S3CopyTaskKeys.SOURCE_KEY]
        self.source_etag = state[S3CopyTaskKeys.SOURCE_ETAG]
        self.destination_bucket = state[S3CopyTaskKeys.DESTINATION_BUCKET]
        self.destination_key = state[S3CopyTaskKeys.DESTINATION_KEY]
        self.upload_id = state[S3CopyTaskKeys.UPLOAD_ID]
        self.size = state[S3CopyTaskKeys.SIZE]
        self.part_size = state[S3CopyTaskKeys.PART_SIZE]
        self.next_part = state[S3CopyTaskKeys.NEXT_PART]
        self.part_count = state[S3CopyTaskKeys.PART_COUNT]

        self.s3_blobstore = S3BlobStore()
        self.queue = collections.deque()  # type: typing.Deque[int]

        self.fetch_size = fetch_size

    @staticmethod
    def setup_copy_task(
            source_bucket: str, source_key: str,
            destination_bucket: str, destination_key: str,
            part_size_calculator: typing.Callable[[int], int]) -> dict:
        """
        Returns the initial state for a S3CopyTask to copy a blob from s3://`source_bucket`/`source_key` to
        s3://`destination_bucket`/`destination_key`.  The work is broken up into parts of size N, where N is provided
        by a callable `part_size_calculator` that's given the total blob size.
        """
        s3_blobstore = S3BlobStore()
        blobinfo = s3_blobstore.get_all_metadata(source_bucket, source_key)
        source_etag = blobinfo['ETag'].strip("\"")  # the ETag is returned with an extra set of quotes.
        source_size = blobinfo['ContentLength']  # type: int
        part_size = part_size_calculator(source_size)
        part_count = source_size // part_size
        if part_count * part_size < source_size:
            part_count += 1
        if part_count > 1:
            mpu = s3_blobstore.s3_client.create_multipart_upload(Bucket=destination_bucket, Key=destination_key)
            upload_id = mpu['UploadId']
        else:
            upload_id = None

        return {
            S3CopyTaskKeys.SOURCE_BUCKET: source_bucket,
            S3CopyTaskKeys.SOURCE_KEY: source_key,
            S3CopyTaskKeys.SOURCE_ETAG: source_etag,
            S3CopyTaskKeys.DESTINATION_BUCKET: destination_bucket,
            S3CopyTaskKeys.DESTINATION_KEY: destination_key,
            S3CopyTaskKeys.UPLOAD_ID: upload_id,
            S3CopyTaskKeys.SIZE: source_size,
            S3CopyTaskKeys.PART_SIZE: part_size,
            S3CopyTaskKeys.NEXT_PART: 1,
            S3CopyTaskKeys.PART_COUNT: part_count,
        }

    def get_state(self) -> dict:
        return {
            S3CopyTaskKeys.SOURCE_BUCKET: self.source_bucket,
            S3CopyTaskKeys.SOURCE_KEY: self.source_key,
            S3CopyTaskKeys.SOURCE_ETAG: self.source_etag,
            S3CopyTaskKeys.DESTINATION_BUCKET: self.destination_bucket,
            S3CopyTaskKeys.DESTINATION_KEY: self.destination_key,
            S3CopyTaskKeys.UPLOAD_ID: self.upload_id,
            S3CopyTaskKeys.SIZE: self.size,
            S3CopyTaskKeys.PART_SIZE: self.part_size,
            S3CopyTaskKeys.NEXT_PART: self.next_part,
            S3CopyTaskKeys.PART_COUNT: self.part_count,
        }

    @property
    def expected_max_one_unit_runtime_millis(self) -> int:
        # expect that in the worst case, we take 60 seconds to copy one part.
        return 60 * 1000

    def run_one_unit(self) -> typing.Optional[typing.Any]:
        if self.part_count == 1:
            # it's not a multipart copy.
            self.s3_blobstore.copy(
                self.source_bucket, self.source_key,
                self.destination_bucket, self.destination_key)

            return True

        if len(self.queue) == 0 and self.next_part <= self.part_count:
            self.queue.extend(
                self.s3_blobstore.find_next_missing_parts(
                    self.destination_bucket,
                    self.destination_key,
                    self.upload_id,
                    self.part_count,
                    self.next_part,
                    self.fetch_size))

        if len(self.queue) == 0 or self.next_part > self.part_count:
            # get all the components
            s3_resource = boto3.resource("s3")

            mpu = s3_resource.MultipartUpload(
                self.destination_bucket, self.destination_key, self.upload_id)

            parts = list(mpu.parts.all())

            assert(len(parts) == self.part_count)
            parts_list = [dict(ETag=part.e_tag,
                               PartNumber=part.part_number)
                          for part in parts
                          ]

            # verify that the ETag of the output file will match the source etag.
            bin_md5 = b"".join([binascii.unhexlify(part.e_tag.strip("\""))
                               for part in parts])
            composite_etag = hashlib.md5(bin_md5).hexdigest() + "-" + str(len(parts))
            assert composite_etag == self.source_etag

            mpu.complete(MultipartUpload=dict(Parts=parts_list))

            return True

        part_id = self.queue[0]

        byte_range = self.calculate_range_for_part(part_id)

        self.s3_blobstore.s3_client.upload_part_copy(
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
        self.next_part = part_id + 1

        self.queue.popleft()

        return None

    def calculate_range_for_part(self, part_id) -> typing.Tuple[int, int]:
        """Calculate the byte range for `part_id`.  Assume these are S3 part IDs, which are 1-indexed."""
        start = (part_id - 1) * self.part_size
        end = part_id * self.part_size
        if end >= self.size:
            end = self.size
        end -= 1

        return start, end


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


# intuitively, this ought to be an enum, but serializing an enum is way too complicated.
class S3ParallelCopySupervisorTaskKeys:
    SOURCE_BUCKET = "srcbucket"
    SOURCE_KEY = "srckey"
    SOURCE_ETAG = "srcetag"
    DESTINATION_BUCKET = "dstbucket"
    DESTINATION_KEY = "dstkey"
    UPLOAD_ID = "uploadid"
    SIZE = "size"
    PART_SIZE = "partsz"
    NEXT_PART = "next"
    PART_COUNT = "count"
    BATCH_SIZE = "batch_size"
    STAGE = "stage"
    TIMEOUT = "timeout"

    class S3ParallelCopySupervisorTaskStages:
        INITIAL = "initial"
        SPAWNING_WORKERS = "spawning"
        WAITING_FOR_WORKERS = "waiting"


class S3ParallelCopySupervisorTask(Task[dict, typing.Any]):
    """
    This is a chunked task that supervises the copying of a multipart copy from one blob to another.
    """
    def __init__(self, state: dict, runtime: Runtime, fetch_size: int=100) -> None:
        self.source_bucket = state[S3ParallelCopySupervisorTaskKeys.SOURCE_BUCKET]
        self.source_key = state[S3ParallelCopySupervisorTaskKeys.SOURCE_KEY]
        self.source_etag = state[S3ParallelCopySupervisorTaskKeys.SOURCE_ETAG]
        self.destination_bucket = state[S3ParallelCopySupervisorTaskKeys.DESTINATION_BUCKET]
        self.destination_key = state[S3ParallelCopySupervisorTaskKeys.DESTINATION_KEY]
        self.upload_id = state[S3ParallelCopySupervisorTaskKeys.UPLOAD_ID]
        self.size = state[S3ParallelCopySupervisorTaskKeys.SIZE]
        self.part_size = state[S3ParallelCopySupervisorTaskKeys.PART_SIZE]
        self.next_part = state[S3ParallelCopySupervisorTaskKeys.NEXT_PART]
        self.part_count = state[S3ParallelCopySupervisorTaskKeys.PART_COUNT]
        self.batch_size = state[S3ParallelCopySupervisorTaskKeys.BATCH_SIZE]
        self.stage = state[S3ParallelCopySupervisorTaskKeys.STAGE]
        self.timeout = state[S3ParallelCopySupervisorTaskKeys.TIMEOUT]

        self.runtime = runtime
        self.fetch_size = fetch_size

        self.s3_blobstore = S3BlobStore()
        self.queue = collections.deque()  # type: typing.Deque[int]

        self.waiting_last_checked = None  # type: typing.Optional[float]

    @staticmethod
    def setup_copy_task(
            source_bucket: str, source_key: str,
            destination_bucket: str, destination_key: str,
            part_size_calculator: typing.Callable[[int], int],
            timeout_seconds: int,
            batch_size: int=100,
    ) -> dict:
        """
        Returns the initial state for a S3CopyTask to copy a blob from s3://`source_bucket`/`source_key` to
        s3://`destination_bucket`/`destination_key`.  The work is broken up into parts of size N, where N is provided
        by a callable `part_size_calculator` that's given the total blob size.
        """
        s3_blobstore = S3BlobStore()
        blobinfo = s3_blobstore.get_all_metadata(source_bucket, source_key)
        source_etag = blobinfo['ETag'].strip("\"")  # the ETag is returned with an extra set of quotes.
        source_size = blobinfo['ContentLength']  # type: int
        part_size = part_size_calculator(source_size)
        part_count = source_size // part_size
        if part_count * part_size < source_size:
            part_count += 1
        if part_count > 1:
            mpu = s3_blobstore.s3_client.create_multipart_upload(Bucket=destination_bucket, Key=destination_key)
            upload_id = mpu['UploadId']
        else:
            upload_id = None

        return {
            S3ParallelCopySupervisorTaskKeys.SOURCE_BUCKET: source_bucket,
            S3ParallelCopySupervisorTaskKeys.SOURCE_KEY: source_key,
            S3ParallelCopySupervisorTaskKeys.SOURCE_ETAG: source_etag,
            S3ParallelCopySupervisorTaskKeys.DESTINATION_BUCKET: destination_bucket,
            S3ParallelCopySupervisorTaskKeys.DESTINATION_KEY: destination_key,
            S3ParallelCopySupervisorTaskKeys.UPLOAD_ID: upload_id,
            S3ParallelCopySupervisorTaskKeys.SIZE: source_size,
            S3ParallelCopySupervisorTaskKeys.PART_SIZE: part_size,
            S3ParallelCopySupervisorTaskKeys.NEXT_PART: 1,
            S3ParallelCopySupervisorTaskKeys.PART_COUNT: part_count,
            S3ParallelCopySupervisorTaskKeys.BATCH_SIZE: batch_size,
            S3ParallelCopySupervisorTaskKeys.STAGE:
                S3ParallelCopySupervisorTaskKeys.S3ParallelCopySupervisorTaskStages.INITIAL,
            S3ParallelCopySupervisorTaskKeys.TIMEOUT: time.time() + timeout_seconds,
        }

    def get_state(self) -> dict:
        return {
            S3ParallelCopySupervisorTaskKeys.SOURCE_BUCKET: self.source_bucket,
            S3ParallelCopySupervisorTaskKeys.SOURCE_KEY: self.source_key,
            S3ParallelCopySupervisorTaskKeys.SOURCE_ETAG: self.source_etag,
            S3ParallelCopySupervisorTaskKeys.DESTINATION_BUCKET: self.destination_bucket,
            S3ParallelCopySupervisorTaskKeys.DESTINATION_KEY: self.destination_key,
            S3ParallelCopySupervisorTaskKeys.UPLOAD_ID: self.upload_id,
            S3ParallelCopySupervisorTaskKeys.SIZE: self.size,
            S3ParallelCopySupervisorTaskKeys.PART_SIZE: self.part_size,
            S3ParallelCopySupervisorTaskKeys.NEXT_PART: self.next_part,
            S3ParallelCopySupervisorTaskKeys.PART_COUNT: self.part_count,
            S3ParallelCopySupervisorTaskKeys.BATCH_SIZE: self.batch_size,
            S3ParallelCopySupervisorTaskKeys.STAGE: self.stage,
            S3ParallelCopySupervisorTaskKeys.TIMEOUT: self.timeout,
        }

    @property
    def expected_max_one_unit_runtime_millis(self) -> int:
        # expect that in the worst case, we take 60 seconds to copy one part.
        return 60 * 1000

    def run_one_unit(self) -> typing.Optional[typing.Any]:
        if time.time() > self.timeout:
            # we've waited a long time and it's not happening.  log an error and return.
            return "supervisor timed out waiting for copy to complete"

        if self.part_count == 1:
            # it's not a multipart copy.
            self.s3_blobstore.copy(
                self.source_bucket, self.source_key,
                self.destination_bucket, self.destination_key)

            return True

        # what state are we in?
        if self.stage == S3ParallelCopySupervisorTaskKeys.S3ParallelCopySupervisorTaskStages.INITIAL:
            assert self.next_part == 1
            self.stage = S3ParallelCopySupervisorTaskKeys.S3ParallelCopySupervisorTaskStages.SPAWNING_WORKERS
            return None

        if self.stage == S3ParallelCopySupervisorTaskKeys.S3ParallelCopySupervisorTaskStages.SPAWNING_WORKERS:
            start = self.next_part
            end = min(self.next_part + self.batch_size - 1, self.part_count)

            initial_state = S3ParallelCopyWorkerTask.setup_copy_task(
                self.source_bucket, self.source_key, self.source_etag,
                self.destination_bucket, self.destination_key,
                self.upload_id,
                self.size,
                self.part_size,
                start,
                end,
            )
            self.runtime.schedule_work(S3ParallelCopyWorkerTask, initial_state, True)

            if end == self.part_count:
                # everything is spawned.  move onto the next state.
                self.stage = S3ParallelCopySupervisorTaskKeys.S3ParallelCopySupervisorTaskStages.WAITING_FOR_WORKERS
            else:
                self.next_part = end + 1
            return None

        if self.stage == S3ParallelCopySupervisorTaskKeys.S3ParallelCopySupervisorTaskStages.WAITING_FOR_WORKERS:
            # we don't want to hammer on the copy completion check, so we space it out by at least one second.
            if (self.waiting_last_checked is not None and
                    time.time() < self.waiting_last_checked + 1):
                time.sleep(self.waiting_last_checked + 1 - time.time())

            # which parts are present?
            s3_resource = boto3.resource("s3")

            mpu = s3_resource.MultipartUpload(
                self.destination_bucket, self.destination_key, self.upload_id)

            parts = list(mpu.parts.all())

            if len(parts) < self.part_count:
                self.waiting_last_checked = time.time()
                return None

            # it's all present!
            parts_list = [dict(ETag=part.e_tag,
                               PartNumber=part.part_number)
                          for part in parts
                          ]

            # verify that the ETag of the output file will match the source etag.
            bin_md5 = b"".join([binascii.unhexlify(part.e_tag.strip("\""))
                                for part in parts])
            composite_etag = hashlib.md5(bin_md5).hexdigest() + "-" + str(len(parts))
            assert composite_etag == self.source_etag

            mpu.complete(MultipartUpload=dict(Parts=parts_list))
            return True

        raise ValueError("Unknown state")


# intuitively, this ought to be an enum, but serializing an enum is way too complicated.
class S3ParallelCopyWorkerTaskKeys:
    SOURCE_BUCKET = "srcbucket"
    SOURCE_KEY = "srckey"
    SOURCE_ETAG = "srcetag"
    DESTINATION_BUCKET = "dstbucket"
    DESTINATION_KEY = "dstkey"
    UPLOAD_ID = "uploadid"
    SIZE = "size"
    PART_SIZE = "partsz"
    NEXT_PART = "next"
    LAST_PART = "last"
    CONCURRENT_REQUESTS = "concurrent_requests"


class S3ParallelCopyWorkerTask(Task[dict, bool]):
    """
    This is a chunked task that does the actual work of a multipart copy from one blob to another.
    """

    def __init__(self, state: dict, *args, **kwargs) -> None:
        self.source_bucket = state[S3ParallelCopyWorkerTaskKeys.SOURCE_BUCKET]
        self.source_key = state[S3ParallelCopyWorkerTaskKeys.SOURCE_KEY]
        self.source_etag = state[S3ParallelCopyWorkerTaskKeys.SOURCE_ETAG]
        self.destination_bucket = state[S3ParallelCopyWorkerTaskKeys.DESTINATION_BUCKET]
        self.destination_key = state[S3ParallelCopyWorkerTaskKeys.DESTINATION_KEY]
        self.upload_id = state[S3ParallelCopyWorkerTaskKeys.UPLOAD_ID]
        self.size = state[S3ParallelCopySupervisorTaskKeys.SIZE]
        self.part_size = state[S3ParallelCopyWorkerTaskKeys.PART_SIZE]
        self.next_part = state[S3ParallelCopyWorkerTaskKeys.NEXT_PART]
        self.last_part = state[S3ParallelCopyWorkerTaskKeys.LAST_PART]
        self.concurrent_requests = state[S3ParallelCopyWorkerTaskKeys.CONCURRENT_REQUESTS]

        self.s3_blobstore = S3BlobStore()

        # find all the missing parts
        self.queue = collections.deque(self.s3_blobstore.find_next_missing_parts(
            self.destination_bucket,
            self.destination_key,
            self.upload_id,
            self.last_part,
            self.next_part,
            self.last_part - self.next_part + 1))

    @staticmethod
    def setup_copy_task(
            source_bucket: str, source_key: str, source_etag: str,
            destination_bucket: str, destination_key: str,
            upload_id: str,
            size: int,
            part_size: int,
            next_part: int,
            last_part: int,
            concurrent_requests: int=8,
    ) -> dict:
        """
        Returns the initial state for a S3ParallelCopyWorkerTask to copy a blob from s3://`source_bucket`/`source_key`
        to s3://`destination_bucket`/`destination_key`.

        :param upload_id: The multipart upload id.
        :param size: The total size of the source blob.
        :param part_size: The size of each part (except the last, which may be shorter).
        :param next_part: The first part this task should try to copy.  Note that each time the task is unfrozen, it
                          will check to see which parts still need to be copied.
        :param last_part: The last part this task should try to copy.
        :param concurrent_requests: The number of concurrent copy requests the task will attempt to manage.
        :return:
        """
        return {
            S3ParallelCopyWorkerTaskKeys.SOURCE_BUCKET: source_bucket,
            S3ParallelCopyWorkerTaskKeys.SOURCE_KEY: source_key,
            S3ParallelCopyWorkerTaskKeys.SOURCE_ETAG: source_etag,
            S3ParallelCopyWorkerTaskKeys.DESTINATION_BUCKET: destination_bucket,
            S3ParallelCopyWorkerTaskKeys.DESTINATION_KEY: destination_key,
            S3ParallelCopyWorkerTaskKeys.UPLOAD_ID: upload_id,
            S3ParallelCopyWorkerTaskKeys.SIZE: size,
            S3ParallelCopyWorkerTaskKeys.PART_SIZE: part_size,
            S3ParallelCopyWorkerTaskKeys.NEXT_PART: next_part,
            S3ParallelCopyWorkerTaskKeys.LAST_PART: last_part,
            S3ParallelCopyWorkerTaskKeys.CONCURRENT_REQUESTS: concurrent_requests,
        }

    def get_state(self) -> dict:
        return {
            S3ParallelCopyWorkerTaskKeys.SOURCE_BUCKET: self.source_bucket,
            S3ParallelCopyWorkerTaskKeys.SOURCE_KEY: self.source_key,
            S3ParallelCopyWorkerTaskKeys.SOURCE_ETAG: self.source_etag,
            S3ParallelCopyWorkerTaskKeys.DESTINATION_BUCKET: self.destination_bucket,
            S3ParallelCopyWorkerTaskKeys.DESTINATION_KEY: self.destination_key,
            S3ParallelCopyWorkerTaskKeys.UPLOAD_ID: self.upload_id,
            S3ParallelCopyWorkerTaskKeys.SIZE: self.size,
            S3ParallelCopyWorkerTaskKeys.PART_SIZE: self.part_size,
            S3ParallelCopyWorkerTaskKeys.NEXT_PART: self.next_part,
            S3ParallelCopyWorkerTaskKeys.LAST_PART: self.last_part,
            S3ParallelCopyWorkerTaskKeys.CONCURRENT_REQUESTS: self.concurrent_requests,
        }

    @property
    def expected_max_one_unit_runtime_millis(self) -> int:
        # expect that in the worst case, we take 60 seconds to copy one part.
        return 60 * 1000

    def run_one_unit(self) -> typing.Optional[bool]:
        def copy_one_part(part_id):
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

        if len(self.queue) == 0:
            return True

        futures = list()
        with ThreadPoolExecutor(max_workers=self.concurrent_requests) as executor:
            for ix in range(self.concurrent_requests):
                if len(self.queue) == 0:
                    break

                part_id = self.queue.popleft()
                futures.append(executor.submit(copy_one_part, part_id))

        for future in futures:
            future.result()

        if len(self.queue) == 0:
            return True

        return None

    def calculate_range_for_part(self, part_id) -> typing.Tuple[int, int]:
        """Calculate the byte range for `part_id`.  Assume these are S3 part IDs, which are 1-indexed."""
        start = (part_id - 1) * self.part_size
        end = part_id * self.part_size
        if end >= self.size:
            end = self.size
        end -= 1

        return start, end

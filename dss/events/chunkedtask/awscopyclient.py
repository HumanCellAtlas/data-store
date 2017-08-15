import binascii
import collections
import hashlib
import typing

import boto3

from . import Task
from ...blobstore.s3 import S3BlobStore


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
    def __init__(self, state: dict, fetch_size: int=100) -> None:
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

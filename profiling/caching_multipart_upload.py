
import sys
import typing
import hashlib

from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

_part_cache = dict()

def get_md5(data):
    m = hashlib.md5()
    m.update(data)
    return m.hexdigest()

def multipart_parallel_upload(
        s3_client: typing.Any,
        bucket: str,
        key: str,
        src_file_handle: typing.BinaryIO,
        *,
        part_size: int,
        part_lookup_object: dict=None,
        content_type: str=None,
        metadata: dict=None,
        parallelization_factor=20) -> typing.Sequence[dict]:
    """
    Upload a file object to s3 in parallel.

    `part_lookup_object` may optionally be included when data with similar part layout already exists in s3.
    `part_lookup_object` should contain two keys: `key`, the reference object key, and `handle`, a file handle
    containing the reference data.
        reference data parts: A-B-B-B-B-B-B-B-B-B-B-B-B-C
        upload data parts:    D-B-B-B-B-B-B-B-B-B-B-B-B-E-E-G
    """
    kwargs: dict = dict()
    if content_type is not None:
        kwargs['ContentType'] = content_type
    if metadata is not None:
        kwargs['Metadata'] = metadata
    mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key, **kwargs)

    def _upload_part(data, part_number):
        resp = s3_client.upload_part(
            Body=data,
            Bucket=bucket,
            Key=key,
            PartNumber=part_number,
            UploadId=mpu['UploadId'],
        )
        return resp['ETag']

    def _copy_part(start_part):
        start = (start_part - 1) * part_size
        end = start + part_size - 1
        resp = s3_client.upload_part_copy(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': part_lookup_object['key']},
            Key=key,
            PartNumber=part_number,
            UploadId=mpu['UploadId'],
            CopySourceRange=f"bytes={start}-{end}",
        )
        return resp['CopyPartResult']['ETag']

    with ThreadPoolExecutor(max_workers=parallelization_factor) as e:
        futures = dict()
        for part_number in range(1, 100000):
            data = src_file_handle.read(part_size)
            if not data:
                break
            old_data = None
            if part_lookup_object is not None:
                old_data = part_lookup_object['handle'].read(part_size)
            if data == old_data:
                futures[e.submit(_copy_part, part_number)] = part_number
            else:
                futures[e.submit(_upload_part, data, part_number)] = part_number
            part_number += 1
        parts = sorted(
            [dict(ETag=f.result(), PartNumber=futures[f]) for f in as_completed(futures)],
            key=lambda p: p['PartNumber']
        )
    s3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        MultipartUpload=dict(Parts=parts),
        UploadId=mpu['UploadId'],
    )
    return parts

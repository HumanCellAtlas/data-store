import os
import boto3
# TODO: renaim dss.util.time to avoid conflict with built in module
import time as python_time
import tempfile
from urllib.parse import SplitResult, parse_qsl, urlencode, urlsplit, urlunsplit
import typing
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed

from dss.storage.identifiers import VERSION_REGEX, UUID_REGEX, TOMBSTONE_SUFFIX


def paginate(boto3_paginator, *args, **kwargs):
    for page in boto3_paginator.paginate(*args, **kwargs):
        for result_key in boto3_paginator.result_keys:
            for value in page.get(result_key.parsed.get("value"), []):
                yield value


class UrlBuilder:
    def __init__(self, url: typing.Optional[str] = None) -> None:
        if url is None:
            self.splitted = SplitResult("", "", "", "", "")
        else:
            self.splitted = urlsplit(url)
        self.query = parse_qsl(self.splitted.query)

    def set(
            self,
            scheme: str = None,
            netloc: str = None,
            path: str = None,
            query: typing.List[typing.Tuple[str, str]] = None,
            fragment: str = None) -> "UrlBuilder":
        kwargs = dict()
        if scheme is not None:
            kwargs['scheme'] = scheme
        if netloc is not None:
            kwargs['netloc'] = netloc
        if path is not None:
            kwargs['path'] = path
        if query is not None:
            self.query = query
        if fragment is not None:
            kwargs['fragment'] = fragment
        self.splitted = self.splitted._replace(**kwargs)

        return self

    def has_query(self, needle_query_name: str) -> bool:
        """Returns True iff the URL being constructed has a query field with name `needle_query_name`."""
        for query_name, _ in self.query:
            if query_name == needle_query_name:
                return True
        return False

    def add_query(self, query_name: str, query_value: str) -> "UrlBuilder":
        self.query.append((query_name, query_value))

        return self

    def replace_query(self, query_name: str, query_value: str) -> "UrlBuilder":
        """
        Given a query name, remove all instances of that query name in this UrlBuilder.  Then append an instance with
        the name set to `query_name` and the value set to `query_value`.
        """
        self.query = [
            (q_name, q_value)
            for q_name, q_value in self.query
            if q_name != query_name
        ]
        self.query.append((query_name, query_value))

        return self

    def __str__(self) -> str:
        result = self.splitted._replace(query=urlencode(self.query, doseq=True))

        return urlunsplit(result)


class RequirementError(RuntimeError):
    """
    Unlike assertions, unsatisfied requirements do not consitute a bug in the program.
    """


def require(condition: bool, *args, exception: type = RequirementError):
    """
    Raise a RequirementError, or an instance of the given exception class, if the given condition is False.

    :param condition: the boolean condition to be required

    :param args: optional positional arguments to be passed to the exception constructor. Typically only one such
                 argument should be provided: a string containing a textual description of the requirement.

    :param exception: a custom exception class to be instantiated and raised if the condition does not hold
    """
    reject(not condition, *args, exception=exception)


def reject(condition: bool, *args, exception: type = RequirementError):
    """
    Raise a RequirementError, or an instance of the given exception class, if the given condition is True.

    :param condition: the boolean condition to be rejected

    :param args: optional positional arguments to be passed to the exception constructor. Typically only one such
                 argument should be provided: a string containing a textual description of the rejected condition.

    :param exception: a custom exception class to be instantiated and raised if the condition occurs
    """
    if condition:
        raise exception(*args)


class hashabledict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))


@lru_cache()
def get_gcp_credentials_file():
    """
    Aquire GCP credentials from AWS secretsmanager and write them to a temporary file.
    A reference to the temporary file is saved in lru_cache so it is not cleaned up
    before a GCP client, which expects a credentials file in the file system, is instantiated.

    Normal usage is local execution. For cloud execution (AWS Lambda, etc.),
    credentials are typically available at GOOGLE_APPLICATION_CREDENTIALS.
    """
    secret_store = os.environ['DSS_SECRETS_STORE']
    stage = os.environ['DSS_DEPLOYMENT_STAGE']
    credentials_secrets_name = os.environ['GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME']
    secret_id = f"{secret_store}/{stage}/{credentials_secrets_name}"
    resp = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)
    tf = tempfile.NamedTemporaryFile("w")
    tf.write(resp['SecretString'])
    tf.flush()
    return tf


def multipart_parallel_upload(
        s3_client: typing.Any,
        bucket: str,
        key: str,
        src_file_handle: typing.BinaryIO,
        *,
        part_size: int,
        content_type: str=None,
        metadata: dict=None,
        parallelization_factor=8) -> typing.Sequence[dict]:
    """
    Upload a file object to s3 in parallel.
    """
    kwargs: dict = dict()
    if content_type is not None:
        kwargs['ContentType'] = content_type
    if metadata is not None:
        kwargs['Metadata'] = metadata
    mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key, **kwargs)

    def _copy_part(data, part_number):
        resp = s3_client.upload_part(
            Body=data,
            Bucket=bucket,
            Key=key,
            PartNumber=part_number,
            UploadId=mpu['UploadId'],
        )
        return resp['ETag']

    def _chunks():
        while True:
            data = src_file_handle.read(part_size)
            if not data:
                break
            yield data

    with ThreadPoolExecutor(max_workers=parallelization_factor) as e:
        futures = {e.submit(_copy_part, data, part_number): part_number
                   for part_number, data in enumerate(_chunks(), start=1)}
        parts = [dict(ETag=f.result(), PartNumber=futures[f]) for f in as_completed(futures)]
        parts.sort(key=lambda p: p['PartNumber'])
    s3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        MultipartUpload=dict(Parts=parts),
        UploadId=mpu['UploadId'],
    )
    return parts

def countdown(maximum_duration: float) -> typing.Iterator[float]:
    """
    Convenience generator to time limit iterated work.
    """
    start_time = python_time.time()
    while True:
        seconds_remaining = maximum_duration - (python_time.time() - start_time)
        if 0 > seconds_remaining:
            break
        yield seconds_remaining

def circular_generator(lst: typing.List[typing.Any]) -> typing.Any:
    """
    Convenience generator to round-robin the elements in a list.
    """
    _i = 0
    while True:
        yield lst[_i]
        _i = (_i + 1) % len(lst)

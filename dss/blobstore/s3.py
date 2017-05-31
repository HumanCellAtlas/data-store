from __future__ import absolute_import, division, print_function, unicode_literals

import typing

import boto3
import botocore

from . import BlobStore, BlobContainerNotFoundError, BlobStoreCredentialError


class S3BlobStore(BlobStore):
    def __init__(self, container: str) -> None:
        super(S3BlobStore, self).__init__()

        self.s3_client = boto3.client("s3")

        try:
            self.s3_client.head_bucket(Bucket=container)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                raise BlobContainerNotFoundError()
            elif error_code == 403:
                raise BlobStoreCredentialError()

    def list(self, prefix: str=None):
        pass

    def get_presigned_url(self, objname: str):
        pass

    def set(self, objname: str, src_file_handle: typing.BinaryIO):
        pass

    def delete(self, objname: str):
        pass

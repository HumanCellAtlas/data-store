from __future__ import absolute_import, division, print_function, unicode_literals

import boto3
import botocore

from .blobstore import *


class S3BlobStore(BlobStore):
    def __init__(self, container, access_key, secret_key):
        super(S3BlobStore, self).__init__()

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

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

    def list(self, prefix=None):
        return [item.name for item in self.bucket]

    def get_presigned_url(self, objname):
        pass

    def set(self, objname, src_file_handle):
        pass

    def delete(self, objname):
        pass

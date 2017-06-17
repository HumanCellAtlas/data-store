from __future__ import absolute_import, division, print_function, unicode_literals

import boto3
import botocore
import requests
import typing

from . import (
    BlobNotFoundError,
    BlobStore,
    BlobStoreCredentialError,
    BlobStoreUnknownError,
)


class S3BlobStore(BlobStore):
    def __init__(self) -> None:
        super(S3BlobStore, self).__init__()

        # verify that the credentials are valid.
        try:
            boto3.client('sts').get_caller_identity()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "InvalidClientTokenId":
                raise BlobStoreCredentialError()

        self.s3_client = boto3.client("s3")

    def list(
            self,
            bucket: str,
            prefix: str=None,
            delimiter: str=None,
            batch_size: typing.Optional[int] = None,
    ) -> typing.Iterator[str]:
        """
        Returns an iterator of all blob entries in a bucket that match a given
        prefix.  Do not return any keys that contain the delimiter past the
        prefix.
        """
        next_marker = None
        kwargs_template = dict(
            Bucket=bucket)  # type: typing.Dict[str, typing.Union[str, int]]
        if prefix is not None:
            kwargs_template['Prefix'] = prefix
        if delimiter is not None:
            kwargs_template['Delimiter'] = delimiter
        if batch_size is not None:
            kwargs_template['MaxKeys'] = batch_size

        while True:
            kwargs = kwargs_template.copy()
            if next_marker is not None:
                kwargs['Marker'] = next_marker

            response = self.s3_client.list_objects(**kwargs)
            for item in response['Contents']:
                yield item['Key']

            if response['IsTruncated']:
                next_marker = response.get(
                    'NextMarker', response['Contents'][-1]['Key'])
            else:
                return

    def generate_presigned_url(
            self,
            bucket: str,
            object_name: str,
            method: str,
            **kwargs):
        args = kwargs.copy()
        args['Bucket'] = bucket
        args['Key'] = object_name
        return self.s3_client.generate_presigned_url(
            ClientMethod=method,
            Params=args,
        )

    def upload_file_handle(
            self,
            bucket: str,
            object_name: str,
            src_file_handle: typing.BinaryIO):
        self.s3_client.upload_fileobj(
            src_file_handle,
            Bucket=bucket,
            Key=object_name,
        )

    def delete(self, bucket: str, object_name: str):
        self.s3_client.delete_object(
            Bucket=bucket,
            key=object_name
        )

    def get(self, bucket: str, object_name: str):
        """
        Retrieves the data for a given object in a given bucket.
        :param bucket: the bucket the object resides in.
        :param object_name: the name of the object for which metadata is being
        retrieved.
        :return: the data
        """
        try:
            response = self.s3_client.get_object(
                Bucket=bucket,
                Key=object_name
            )
            return response['Body'].read()
        except botocore.exceptions.ClientError as ex:
            if ex.response['Error']['Code'] == "NoSuchKey":
                raise BlobNotFoundError(ex)
            raise BlobStoreUnknownError(ex)

    def get_metadata(self, bucket: str, object_name: str):
        """
        Retrieves the metadata for a given object in a given bucket.  If the
        platform has any mandatory prefixes or suffixes for the metadata keys,
        they should be stripped before being returned.
        :param bucket: the bucket the object resides in.
        :param object_name: the name of the object for which metadata is being
        retrieved.
        :return: a dictionary mapping metadata keys to metadata values.
        """
        try:
            response = self.s3_client.head_object(
                Bucket=bucket,
                Key=object_name
            )
            return response['Metadata']
        except botocore.exceptions.ClientError as ex:
            if int(ex.response['Error']['Code']) == \
                    int(requests.codes.not_found):
                raise BlobNotFoundError(ex)
            raise BlobStoreUnknownError(ex)

    def copy(
            self,
            src_bucket: str, src_object_name: str,
            dst_bucket: str, dst_object_name: str,
            **kwargs
    ):
        self.s3_client.copy(
            dict(
                Bucket=src_bucket,
                Key=src_object_name,
            ),
            Bucket=dst_bucket,
            Key=dst_object_name,
            ExtraArgs=kwargs,
        )

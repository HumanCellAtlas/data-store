import boto3
import botocore
import requests
import typing

from boto3.s3.transfer import TransferConfig

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
    ) -> typing.Iterator[str]:
        """
        Returns an iterator of all blob entries in a bucket that match a given prefix.  Do not return any keys that
        contain the delimiter past the prefix.
        """
        kwargs = dict()
        if prefix is not None:
            kwargs['Prefix'] = prefix
        if delimiter is not None:
            kwargs['Delimiter'] = delimiter
        for item in (
                boto3.resource("s3").Bucket(bucket).
                objects.
                filter(**kwargs)):
            yield item.key

    def generate_presigned_GET_url(
            self,
            bucket: str,
            object_name: str,
            **kwargs) -> str:
        return self._generate_presigned_url(
            bucket,
            object_name,
            "get_object"
        )

    def _generate_presigned_url(
            self,
            bucket: str,
            object_name: str,
            method: str,
            **kwargs) -> str:
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

    def get(self, bucket: str, object_name: str) -> bytes:
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

    def get_cloud_checksum(
            self,
            bucket: str,
            object_name: str
    ) -> str:
        """
        Retrieves the cloud-provided checksum for a given object in a given bucket.
        :param bucket: the bucket the object resides in.
        :param object_name: the name of the object for which checksum is being retrieved.
        :return: the cloud-provided checksum
        """
        try:
            response = self.s3_client.head_object(
                Bucket=bucket,
                Key=object_name
            )
            # hilariously, the ETag is quoted.  Unclear why.
            return response['ETag'].strip("\"")
        except botocore.exceptions.ClientError as ex:
            if str(ex.response['Error']['Code']) == \
                    str(requests.codes.not_found):
                raise BlobNotFoundError(ex)
            raise BlobStoreUnknownError(ex)

    def get_user_metadata(
            self,
            bucket: str,
            object_name: str
    ) -> typing.Dict[str, str]:
        """
        Retrieves the user metadata for a given object in a given bucket.  If the platform has any mandatory prefixes or
        suffixes for the metadata keys, they should be stripped before being returned.
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
            metadata = response['Metadata'].copy()

            response = self.s3_client.get_object_tagging(
                Bucket=bucket,
                Key=object_name,
            )
            for tag in response['TagSet']:
                key, value = tag['Key'], tag['Value']
                metadata[key] = value

            return metadata
        except botocore.exceptions.ClientError as ex:
            if str(ex.response['Error']['Code']) == \
                    str(requests.codes.not_found):
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
            Config=TransferConfig(
                multipart_threshold=64 * 1024 * 1024,
                multipart_chunksize=64 * 1024 * 1024,
            ),
        )

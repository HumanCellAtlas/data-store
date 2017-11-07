import boto3
import botocore
import requests
import typing

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

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
        self.s3 = boto3.resource('s3')

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

    def get_all_metadata(
            self,
            bucket: str,
            object_name: str
    ) -> dict:
        """
        Retrieves all the metadata for a given object in a given bucket.
        :param bucket: the bucket the object resides in.
        :param object_name: the name of the object for which metadata is being retrieved.
        :return: the metadata
        """
        try:
            return self.s3_client.head_object(
                Bucket=bucket,
                Key=object_name
            )
        except botocore.exceptions.ClientError as ex:
            if str(ex.response['Error']['Code']) == \
                    str(requests.codes.not_found):
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
        response = self.get_all_metadata(bucket, object_name)
        # hilariously, the ETag is quoted.  Unclear why.
        return response['ETag'].strip("\"")

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
            response = self.get_all_metadata(bucket, object_name)
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

    def find_next_missing_parts(
            self,
            bucket: str,
            key: str,
            upload_id: str,
            part_count: int,
            search_start: int=1,
            return_count: int=1) -> typing.Sequence[int]:
        """
        Given a `bucket`, `key`, and `upload_id`, find the next N missing parts of a multipart upload, where
        N=`return_count`.  If `search_start` is provided, start the search at part M, where M=`search_start`.
        `part_count` is the number of parts expected for the upload.

        Note that the return value may contain fewer than N parts.
        """
        if part_count < search_start:
            raise ValueError("")
        result = list()
        while True:
            kwargs = dict(Bucket=bucket, Key=key, UploadId=upload_id)  # type: dict
            if search_start > 1:
                kwargs['PartNumberMarker'] = search_start - 1

            # retrieve all the parts after the one we *think* we need to start from.
            parts_resp = self.s3_client.list_parts(**kwargs)

            # build a set of all the parts known to be uploaded, detailed in this request.
            parts_map = set()  # type: typing.Set[int]
            for part_detail in parts_resp.get('Parts', []):
                parts_map.add(part_detail['PartNumber'])

            while True:
                if search_start not in parts_map:
                    # not found, add it to the list of parts we still need.
                    result.append(search_start)

                # have we met our requirements?
                if len(result) == return_count or search_start == part_count:
                    return result

                search_start += 1

                if parts_resp['IsTruncated'] and search_start == parts_resp['NextPartNumberMarker']:
                    # finished examining the results of this batch, move onto the next one
                    break

    def check_bucket_exists(self, bucket: str) -> bool:
        """
        Checks if bucket with specified name exists.
        :param bucket: the bucket to be checked.
        :return: true if specified bucket exists in the AZ.
        """
        exists = True
        try:
            self.s3.meta.client.head_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
        return exists

    public_acl_indicator = 'http://acs.amazonaws.com/groups/global/AllUsers'

    def check_bucket_permissions(self, bucket: str, permissions_to_check:[str]) -> bool:
        """
        Checks if bucket with specified name exists.
        :param bucket: the bucket to be checked.
        :param permissions_to_check: array of persmissions to check e.g. ['READ', 'WRITE']
        :return: true if specified permissions are granted to public.
        """
        bucket_acl = self.s3_client.get_bucket_acl(Bucket=bucket)

        for grants in bucket_acl['Grants']:
            for (k, v) in grants.items():
                if k == 'Permission' and any(permission in v for permission in permissions_to_check):
                    for (grantee_attrib_k, grantee_attrib_v) in grants['Grantee'].items():
                        if 'URI' in grantee_attrib_k and grants['Grantee']['URI'] == S3BlobStore.public_acl_indicator:
                            return True
        return False

    def get_bucket_region(self, bucket) -> str:
        """
        Get region associated with a specified bucket name.
        :param bucket: the bucket to be checked.
        :return: region, Note that underying AWS API returns None for default US-East-1, I'm replacing that with us-east-1.
        """
        region = self.s3.meta.client.get_bucket_location(Bucket=bucket)["LocationConstraint"]
        return 'us-east-1' if region is None else region

    test_file = 'touch/testfile.txt'
    def touch_test_file(self, bucket) -> (bool, str):
        """
        Write a test file into the specified bucket.
        :param bucket: the bucket to be checked.
        :return: True if able to write, if not also returns error message as a cause
        """
        result = False
        cause = None
        try:
            self.s3.Object(bucket, S3BlobStore.test_file).put(Body='Test', Metadata={'foo': 'bar'})
            result = True
        except Exception as e:
            cause = "Error {0}".format(str(e))
        return result, cause


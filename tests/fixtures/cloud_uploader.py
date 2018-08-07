import logging
import os
import typing

import boto3
from boto3.s3.transfer import TransferConfig
from google.cloud.storage import Client

from dcplib.s3_multipart import MULTIPART_THRESHOLD, get_s3_multipart_chunk_size
from dcplib.checksumming_io import ChecksummingSink


logger = logging.getLogger(__name__)


class Uploader:
    def __init__(self, local_root: str, *args, **kwargs) -> None:
        self.local_root = local_root

    def reset(self) -> None:
        raise NotImplementedError()

    def upload_file(
            self,
            local_path: str,
            remote_path: str,
            content_type: str="application/octet-stream",
            metadata_keys: typing.Dict[str, str]=None,
            *args,
            **kwargs) -> None:
        raise NotImplementedError()

    def checksum_and_upload_file(
            self,
            local_path: str,
            remote_path: str,
            content_type: str="application/octet-stream",
            metadata: typing.Dict[str, str]=None,
            *args,
            **kwargs
    ) -> None:
        if metadata is None:
            metadata = dict()

        fpath = os.path.join(self.local_root, local_path)
        size = os.path.getsize(fpath)
        chunk_size = get_s3_multipart_chunk_size(size)
        with ChecksummingSink(write_chunk_size=chunk_size) as sink, open(fpath, "rb") as fh:
            data = fh.read()
            sink.write(data)

            sums = sink.get_checksums()

        metadata['hca-dss-crc32c'] = sums['crc32c'].lower()
        metadata['hca-dss-s3_etag'] = sums['s3_etag'].lower()
        metadata['hca-dss-sha1'] = sums['sha1'].lower()
        metadata['hca-dss-sha256'] = sums['sha256'].lower()

        self.upload_file(local_path, remote_path, content_type, metadata, *args, **kwargs)  # noqa


class S3Uploader(Uploader):
    def __init__(self, local_root: str, bucket: str) -> None:
        super(S3Uploader, self).__init__(local_root)
        self.bucket = bucket
        self.s3_client = boto3.client('s3')

    def reset(self) -> None:
        logger.info(f"Emptying bucket: s3://{self.bucket}")
        s3 = boto3.resource('s3')
        s3.Bucket(self.bucket).objects.delete()

    def upload_file(
            self,
            local_path: str,
            remote_path: str,
            content_type: str="application/octet-stream",
            metadata_keys: typing.Dict[str, str]=None,
            tags: typing.Dict[str, str]=None,
            *args,
            **kwargs) -> None:
        if metadata_keys is None:
            metadata_keys = dict()
        if tags is None:
            tags = dict()

        fp = os.path.join(self.local_root, local_path)
        sz = os.stat(fp).st_size

        chunk_sz = get_s3_multipart_chunk_size(sz)
        transfer_config = TransferConfig(
            multipart_threshold=MULTIPART_THRESHOLD,
            multipart_chunksize=chunk_sz,
        )

        logger.info(f"Uploading {local_path} to s3://{self.bucket}/{remote_path}")
        self.s3_client.upload_file(
            fp,
            self.bucket,
            remote_path,
            ExtraArgs={
                "Metadata": metadata_keys,
                "ContentType": content_type,
            },
            Config=transfer_config,
        )

        tagset = dict(TagSet=[])  # type: typing.Dict[str, typing.List[dict]]
        for tag_key, tag_value in tags.items():
            tagset['TagSet'].append(
                dict(
                    Key=tag_key,
                    Value=tag_value))
        self.s3_client.put_object_tagging(Bucket=self.bucket,
                                          Key=remote_path,
                                          Tagging=tagset)


class GSUploader(Uploader):
    def __init__(self, local_root: str, bucket_name: str) -> None:
        super(GSUploader, self).__init__(local_root)
        credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        self.gcp_client = Client.from_service_account_json(credentials)
        self.bucket = self.gcp_client.bucket(bucket_name)

    def reset(self) -> None:
        logger.info(f"Emptying bucket: gs://{self.bucket.name}")
        for blob in self.bucket.list_blobs():
            blob.delete()

    def upload_file(
            self,
            local_path: str,
            remote_path: str,
            content_type: str="application/octet-stream",
            metadata_keys: typing.Dict[str, str]=None,
            *args,
            **kwargs) -> None:
        logger.info(f"Uploading {local_path} to gs://{self.bucket.name}/{remote_path}")
        blob = self.bucket.blob(remote_path)
        blob.upload_from_filename(os.path.join(self.local_root, local_path), content_type=content_type)
        if metadata_keys:
            blob.metadata = metadata_keys
            blob.patch()

from __future__ import absolute_import, division, print_function, unicode_literals

import typing

from . import HCABlobStore
from ..blobstore import BlobStore


class S3HCABlobStore(HCABlobStore):
    def __init__(self, handle: BlobStore) -> None:
        self.handle = handle

    def copy_blob_from_staging(
        self,
        src_bucket: str, src_object_name: str,
        dst_bucket: str, dst_object_name: str,
    ):
        # retrieve the metadata first.
        source_metadata = self.handle.get_metadata(
            src_bucket, src_object_name)

        # build up the dict for executing the copy.
        kwargs = dict()  # type: typing.Dict[str, typing.Any]
        kwargs['CopySourceIfMatch'] = source_metadata['hca-dss-s3_etag']
        kwargs['Metadata'] = dict()
        for metadata_key in HCABlobStore.COPIED_METADATA:
            kwargs['Metadata'][metadata_key] = source_metadata[metadata_key]
        kwargs['MetadataDirective'] = "REPLACE"

        self.handle.copy(
            src_bucket, src_object_name,
            dst_bucket, dst_object_name,
            **kwargs)

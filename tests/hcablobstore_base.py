import json

from cloud_blobstore import BlobNotFoundError

from tests.infra import testmode


@testmode.integration
class HCABlobStoreTests:
    """
    Common HCA blobstore tests.  We want to avoid repeating ourselves, so if we
    built the abstractions correctly, common operations can all be tested here.
    """

    def test_verify_blob_checksum_from_staging_metadata(self):
        bucket = self.test_fixtures_bucket
        key = "test_good_source_data/0"
        self.assertTrue(
            self.hcahandle.verify_blob_checksum_from_staging_metadata(
                bucket, key,
                self.blobhandle.get_user_metadata(bucket, key)))

        key = "test_bad_source_data/incorrect_checksum"
        self.assertFalse(
            self.hcahandle.verify_blob_checksum_from_staging_metadata(
                bucket, key,
                self.blobhandle.get_user_metadata(bucket, key)))

        key = "DOES_NOT_EXIST"
        with self.assertRaises(BlobNotFoundError):
            self.hcahandle.verify_blob_checksum_from_staging_metadata(
                bucket, key, {})

    def test_verify_blob_checksum_from_dss_metadata(self):
        bucket = self.test_fixtures_bucket
        key = ("blobs/cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30."
               "2b8b815229aa8a61e483fb4ba0588b8b6c491890.3b83ef96387f14655fc854ddc3c6bd57.e16e07b9")
        bundle_key = "bundles/011c7340-9b3c-4d62-bf49-090d79daf198.2017-06-20T214506.766634Z"
        bundle = json.loads(self.blobhandle.get(bucket, bundle_key))
        self.assertTrue(
            self.hcahandle.verify_blob_checksum_from_dss_metadata(
                bucket, key, bundle['files'][0]))

        key = ("blobs/9cdc9050cecf59381fed55a2433140b69596fc861bee55abeafd1f9150f3e2da."
               "15684690e8132044f378b4d4af8a7331c8da17b1.7f54939b30ae7b6d45d473a4c82a41b0.114dee2c")
        self.assertFalse(
            self.hcahandle.verify_blob_checksum_from_dss_metadata(
                bucket, key, bundle['files'][0]))

        key = "DOES_NOT_EXIST"
        with self.assertRaises(BlobNotFoundError):
            self.hcahandle.verify_blob_checksum_from_dss_metadata(
                bucket, key, bundle['files'][0])

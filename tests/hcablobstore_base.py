from cloud_blobstore import BlobNotFoundError


class HCABlobStoreTests:
    """
    Common HCA blobstore tests.  We want to avoid repeating ourselves, so if we
    built the abstractions correctly, common operations can all be tested here.
    """

    def test_verify_blob_checksum(self):
        bucket = self.test_fixtures_bucket
        key = "test_good_source_data/0"
        self.assertTrue(
            self.hcahandle.verify_blob_checksum(
                bucket, key,
                self.blobhandle.get_user_metadata(bucket, key)))

        key = "test_bad_source_data/incorrect_checksum"
        self.assertFalse(
            self.hcahandle.verify_blob_checksum(
                bucket, key,
                self.blobhandle.get_user_metadata(bucket, key)))

        key = "DOES_NOT_EXIST"
        with self.assertRaises(BlobNotFoundError):
            self.hcahandle.verify_blob_checksum(
                bucket, key, {})

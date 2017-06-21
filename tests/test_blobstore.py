from dss.blobstore import BlobNotFoundError, BlobStore


class BlobStoreTests(object):
    """
    Common blobstore tests.  We want to avoid repeating ourselves, so if we
    built the abstractions correctly, common operations can all be tested here.
    """

    def test_get_metadata(self):
        """
        Ensure that the ``get_metadata`` methods return sane data.
        """
        handle = self.handle  # type: BlobStore
        metadata = handle.get_metadata(
            self.test_src_data_bucket,
            "test_good_source_data/0")
        self.assertIn('hca-dss-content-type', metadata)

        with self.assertRaises(BlobNotFoundError):
            handle.get_metadata(
                self.test_src_data_bucket,
                "test_good_source_data_DOES_NOT_EXIST")

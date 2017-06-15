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
            self.test_container,
            "sourcedata/test_good_source_data")
        self.assertIn('hca-dss-content-type', metadata)

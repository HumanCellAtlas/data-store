import io

import requests

from dss.blobstore import BlobNotFoundError, BlobStore
from tests import infra


class BlobStoreTests:
    """
    Common blobstore tests.  We want to avoid repeating ourselves, so if we
    built the abstractions correctly, common operations can all be tested here.
    """

    def test_get_metadata(self):
        """
        Ensure that the ``get_metadata`` methods return sane data.
        """
        handle = self.handle  # type: BlobStore
        metadata = handle.get_user_metadata(
            self.test_fixtures_bucket,
            "test_good_source_data/0")
        self.assertIn('hca-dss-content-type', metadata)

        with self.assertRaises(BlobNotFoundError):
            handle.get_user_metadata(
                self.test_fixtures_bucket,
                "test_good_source_data_DOES_NOT_EXIST")

    def testList(self):
        """
        Ensure that the ```list``` method returns sane data.
        """
        items = [item for item in
                 self.handle.list(
                     self.test_fixtures_bucket,
                     "test_good_source_data/0",
                 )]
        self.assertTrue(len(items) > 0)
        for item in items:
            if item == "test_good_source_data/0":
                break
        else:
            self.fail("did not find the requisite key")

        # fetch a bunch of items all at once.
        items = [item for item in
                 self.handle.list(
                     self.test_fixtures_bucket,
                     "testList/prefix",
                 )]
        self.assertEqual(len(items), 10)

        # this should fetch both testList/delimiter and testList/delimiter/test
        items = [item for item in
                 self.handle.list(
                     self.test_fixtures_bucket,
                     "testList/delimiter",
                 )]
        self.assertEqual(len(items), 2)

        # this should fetch only testList/delimiter
        items = [item for item in
                 self.handle.list(
                     self.test_fixtures_bucket,
                     "testList/delimiter",
                     delimiter="/"
                 )]
        self.assertEqual(len(items), 1)

    def testGetPresignedUrl(self):
        presigned_url = self.handle.generate_presigned_GET_url(
            self.test_fixtures_bucket,
            "test_good_source_data/0",
        )

        resp = requests.get(presigned_url)
        self.assertEqual(resp.status_code, requests.codes.ok)

    def testUploadFileHandle(self):
        fobj = io.BytesIO(b"abcabcabc")
        dst_blob_name = infra.generate_test_key()

        self.handle.upload_file_handle(
            self.test_bucket,
            dst_blob_name,
            fobj
        )

        # should be able to get metadata for the file.
        self.handle.get_user_metadata(
            self.test_bucket, dst_blob_name)

    def testGet(self):
        data = self.handle.get(
            self.test_fixtures_bucket,
            "test_good_source_data/0",
        )
        self.assertEqual(len(data), 11358)

        with self.assertRaises(BlobNotFoundError):
            self.handle.get(
                self.test_fixtures_bucket,
                "test_good_source_data_DOES_NOT_EXIST",
            )

    def testCopy(self):
        dst_blob_name = infra.generate_test_key()

        self.handle.copy(
            self.test_fixtures_bucket,
            "test_good_source_data/0",
            self.test_bucket,
            dst_blob_name,
        )

        # should be able to get metadata for the file.
        self.handle.get_user_metadata(
            self.test_bucket, dst_blob_name)

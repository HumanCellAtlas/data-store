import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.storage.identifiers import (ObjectIdentifier, FileFQID, BundleTombstoneID, BundleFQID, CollectionFQID,
                                     CollectionTombstoneID, BUNDLE_PREFIX, FILE_PREFIX, COLLECTION_PREFIX)
from tests.infra import testmode


@testmode.standalone
class TestObjectIdentifier(unittest.TestCase):

    def test_to_str(self):
        uuid = "0ddba11-0000-4a6b-8f0d-a7d2105c23be"
        version = "2017-12-05T235728.441373Z"
        tests = [BundleFQID, FileFQID, CollectionFQID]
        for identity in tests:
            with self.subTest(identity.__name__):
                self.assertEquals(
                    str(identity(uuid=uuid, version=version)),
                    f"{uuid}.{version}"
                )

        tests = [CollectionTombstoneID, BundleTombstoneID]
        for identity in tests:
            with self.subTest(identity.__name__):
                self.assertEquals(
                    str(identity(uuid=uuid, version=version)),
                    f"{uuid}.{version}.dead"
                )
                self.assertEquals(
                    str(identity(uuid=uuid, version=None)),
                    f"{uuid}.dead"
                )

    def test_from_key(self):
        """
        Test that the from key method correctly returns the right types of identifiers
        """
        uuid = "ca11ab1e-0000-4a6b-8f0d-a7d2105c23be"
        version = "2017-12-05T235728.441373Z"
        self.assertEquals(
            BundleFQID(uuid, version),
            ObjectIdentifier.from_key(f"{BUNDLE_PREFIX}/{uuid}.{version}"),
        )
        self.assertEquals(
            FileFQID(uuid, version),
            ObjectIdentifier.from_key(f"{FILE_PREFIX}/{uuid}.{version}"),
        )
        self.assertEquals(
            CollectionFQID(uuid, version),
            ObjectIdentifier.from_key(f"{COLLECTION_PREFIX}/{uuid}.{version}"),
        )
        self.assertEquals(
            CollectionTombstoneID(uuid, version),
            ObjectIdentifier.from_key(f"{COLLECTION_PREFIX}/{uuid}.{version}.dead"),
        )
        self.assertEquals(
            BundleTombstoneID(uuid, version),
            ObjectIdentifier.from_key(f"{BUNDLE_PREFIX}/{uuid}.{version}.dead"),
        )
        self.assertRaises(
            ValueError,
            lambda: ObjectIdentifier.from_key(f"{BUNDLE_PREFIX}/trash"),
        )
        self.assertRaises(
            ValueError,
            lambda: ObjectIdentifier.from_key(f"trash/{uuid}.{version}.dead"),
        )

    def test_to_key(self):
        uuid = "0ddba11-0000-4a6b-8f0d-a7d2105c23be"
        version = "2017-12-05T235728.441373Z"
        tests = [(BundleFQID, BUNDLE_PREFIX), (FileFQID, FILE_PREFIX), (CollectionFQID, COLLECTION_PREFIX)]
        for identity, prefix in tests:
            with self.subTest(identity.__name__):
                self.assertEquals(
                    identity(uuid=uuid, version=version).to_key(),
                    f"{prefix}/{uuid}.{version}"
                )

        tests = [(CollectionTombstoneID, COLLECTION_PREFIX), (BundleTombstoneID, BUNDLE_PREFIX)]
        for identity, prefix in tests:
            with self.subTest(identity.__name__):
                self.assertEquals(
                    identity(uuid=uuid, version=version).to_key(),
                    f"{prefix}/{uuid}.{version}.dead"
                )
                self.assertEquals(
                    identity(uuid=uuid, version=None).to_key(),
                    f"{prefix}/{uuid}.dead"
                )

    def test_to_key_prefix(self):
        uuid = "0ddba11-0000-4a6b-8f0d-a7d2105c23be"
        version = "2017-12-05T235728.441373Z"
        self.assertEquals(
            BundleFQID(uuid=uuid, version=version).to_key_prefix(),
            f"{BUNDLE_PREFIX}/{uuid}.{version}"
        )
        self.assertEquals(
            BundleFQID(uuid=uuid, version=None).to_key_prefix(),
            f"{BUNDLE_PREFIX}/{uuid}."
        )

    def test_tombstone_is_fully_qualified(self):
        uuid = "0ddba11-0000-4a6b-8f0d-a7d2105c23be"
        version = "2017-12-05T235728.441373Z"
        self.assertTrue(
            BundleTombstoneID(uuid=uuid, version=version).is_fully_qualified(),
        )
        self.assertFalse(
            BundleTombstoneID(uuid=uuid, version=None).is_fully_qualified(),
        )

    def test_tombstone_to_bundle_fqid(self):
        uuid = "0ddba11-0000-4a6b-8f0d-a7d2105c23be"
        version = "2017-12-05T235728.441373Z"
        self.assertTrue(
            BundleTombstoneID(uuid=uuid, version=version).to_fqid(),
            BundleFQID(uuid=uuid, version=version),
        )
        self.assertRaises(
            ValueError,
            lambda: BundleTombstoneID(uuid=uuid, version=None).to_fqid(),
        )


if __name__ == "__main__":
    unittest.main()

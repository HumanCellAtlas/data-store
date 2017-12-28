import os
import sys
import unittest

from tests.infra import testmode

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.storage.bundles import ObjectIdentifier, TombstoneID, FileFQID, BundleFQID, BUNDLE_PREFIX, FILE_PREFIX


@testmode.standalone
class TestObjectIdentifier(unittest.TestCase):

    def test_to_str(self):
        uuid = "0ddba11-0000-4a6b-8f0d-a7d2105c23be"
        version = "2017-12-05T235728.441373Z"
        self.assertEquals(
            str(BundleFQID(uuid=uuid, version=version)),
            f"{uuid}.{version}"
        )
        self.assertEquals(
            str(FileFQID(uuid=uuid, version=version)),
            f"{uuid}.{version}"
        )
        self.assertEquals(
            str(TombstoneID(uuid=uuid, version=version)),
            f"{uuid}.{version}.dead"
        )
        self.assertEquals(
            str(TombstoneID(uuid=uuid, version=None)),
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
            TombstoneID(uuid, version),
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
        self.assertEquals(
            BundleFQID(uuid=uuid, version=version).to_key(),
            f"{BUNDLE_PREFIX}/{uuid}.{version}"
        )
        self.assertEquals(
            FileFQID(uuid=uuid, version=version).to_key(),
            f"{FILE_PREFIX}/{uuid}.{version}"
        )
        self.assertEquals(
            TombstoneID(uuid=uuid, version=version).to_key(),
            f"{BUNDLE_PREFIX}/{uuid}.{version}.dead"
        )
        self.assertEquals(
            TombstoneID(uuid=uuid, version=None).to_key(),
            f"{BUNDLE_PREFIX}/{uuid}.dead"
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
            TombstoneID(uuid=uuid, version=version).is_fully_qualified(),
        )
        self.assertFalse(
            TombstoneID(uuid=uuid, version=None).is_fully_qualified(),
        )

    def test_tombstone_to_bundle_fqid(self):
        uuid = "0ddba11-0000-4a6b-8f0d-a7d2105c23be"
        version = "2017-12-05T235728.441373Z"
        self.assertTrue(
            TombstoneID(uuid=uuid, version=version).to_bundle_fqid(),
            BundleFQID(uuid=uuid, version=version),
        )
        self.assertRaises(
            ValueError,
            lambda: TombstoneID(uuid=uuid, version=None).to_bundle_fqid(),
        )


if __name__ == "__main__":
    unittest.main()

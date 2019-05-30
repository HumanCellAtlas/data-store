#!/usr/bin/env python
# coding: utf-8

import os
import sys
import unittest
from uuid import uuid4

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.storage.identifiers import UUID_REGEX
from tests.infra import testmode


@testmode.standalone
class TestRegexIdentifiers(unittest.TestCase):
    def test_REGEX_MATCHING(self):
        chars = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        for i, c in enumerate(chars):
            uuid = f'{c*8}-{c*4}-{c*4}-{c*4}-{c*12}'
            if i <= 15:
                self.assertTrue(UUID_REGEX.match(uuid), uuid)
            else:
                self.assertIsNone(UUID_REGEX.match(uuid), uuid)

        for i in range(100):
            uuid = str(uuid4())
            self.assertTrue(UUID_REGEX.match(uuid), uuid)


if __name__ == '__main__':
    unittest.main()

#!/usr/bin/env python
# coding: utf-8

import os
import sys
import uuid
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.logging import configure_test_logging
from dss.config import Replica
from tests.infra import testmode
from dss.util.async_state import AsyncStateItem, AsyncStateError

def setUpModule():
    configure_test_logging()

@testmode.standalone
class TestAsyncState(unittest.TestCase):
    def test_item(self):
        key = str(uuid.uuid4())
        msg = "some test msg"

        # Getting a non-existent item should return None
        item = AsyncStateItem.get(key)
        self.assertIsNone(item)

        # insert item
        put_item = AsyncStateItem.put(key, {'message': msg})
        self.assertEqual(put_item.data, {'message': msg})

        # test get item
        item = AsyncStateItem.get(key)
        self.assertEqual(item.body['message'], put_item.body['message'])
        self.assertEqual(item.data, {'message': msg})

        # test delete item
        item.delete_item()
        self.assertIsNone(AsyncStateItem.get(key))

    def test_error(self):
        class TestAsyncError(AsyncStateError):
            pass

        key = str(uuid.uuid4())
        message = "what went wrong"

        # insert item
        TestAsyncError.put(key, message)

        # test get error
        error = AsyncStateItem.get(key)
        self.assertEqual(error.message, message)

        # raise it and catch it
        with self.assertRaises(TestAsyncError):
            raise error

        # test delete item
        error.delete_item()
        self.assertIsNone(TestAsyncError.get(key))


if __name__ == '__main__':
    unittest.main()

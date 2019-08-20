#!/usr/bin/env python
# coding: utf-8

import os
import sys
import uuid
import time
import unittest
import json

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.logging import configure_test_logging
from tests.infra import testmode
from dss.util.async_state import AsyncStateItem, AsyncStateError
from dss.util.aws.clients import dynamodb as db  # type: ignore


def setUpModule():
    configure_test_logging()


@testmode.standalone
class TestAsyncState(unittest.TestCase):
    def test_item_expiration(self):
        key = str(uuid.uuid4())
        msg = "some test msg"
        item = AsyncStateItem(key, {'message': msg})

        # Getting a non-existent item should return None
        self.assertIsNone(item.get(key))

        # insert item
        ten_seconds = int(time.time() + 10)  # expire in 10 seconds
        item._put(expires=ten_seconds)

        # Getting an existent item (within 10 seconds) should return the value
        self.assertEqual(item.get(key).data, {'message': msg})

        # wait for the item to expire
        while time.time() < ten_seconds + 1:
            time.sleep(1)

        # AWS can take up to 48 hours (though often much sooner) to ACTUALLY delete the expired item
        # so we filter for expired items
        query = {
            "TableName": AsyncStateItem.table,
            "ExpressionAttributeNames": {
                "#ttl": "ttl"
            },
            "FilterExpression": "#ttl > :ttl",
            "ExpressionAttributeValues": {
                ":ttl": {"N": str(int(time.time()))}
            }
        }
        r = db.scan(**query)
        for result in r['Items']:
            if result['hash_key']['S'] == key:
                raise RuntimeError(f'Async Item was found to not contain an expired tag!\n'
                                   f'{json.dumps(result, indent=4)}\n'
                                   f'Current time: {str(int(time.time()))}\n'
                                   f'Earlier time: {ten_seconds}\n')

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
        for _ in range(3):
            item = AsyncStateItem.get(key)
            if item is not None:
                break
            else:
                time.sleep(1)
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

#!/usr/bin/env python3

import base64
import datetime
import io
import logging
import os
import sys
import json
import hashlib
import unittest
from uuid import uuid4
from argparse import Namespace
import time
import boto3
#import crcmod
#import google.cloud.storage
from botocore.vendored import requests


#@timeout(1)
class TestCollections(unittest.TestCase):
    def test_collections(self):
        contents = [dict(type="file", uuid="0133c91a-31ac-43b8-9819-84903289064d", version="2018-03-23T164152.102044Z")] * 20
        with self.subTest("Create new collection"):
            res = requests.put("http://localhost:5000/v1/collections",
                               headers=dict(Authorization="Bearer "+os.environ["GOOG_AUTH_TOKEN"]),
                               params=dict(uuid=str(uuid4()), version=datetime.datetime.now().isoformat(), replica="aws"),
                               json=dict(name="n", description="d", details={}, contents=contents))
            print(res)
            print(res.json())
            res.raise_for_status()
            uuid, version = res.json()["uuid"], res.json()["version"]
        with self.subTest("Get created collection"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=dict(Authorization="Bearer "+os.environ["GOOG_AUTH_TOKEN"]),
                               params=dict(version=version, replica="aws"))
            res.raise_for_status()
            assert res.json()["contents"] == contents
        with self.subTest("Get latest version of collection"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=dict(Authorization="Bearer "+os.environ["GOOG_AUTH_TOKEN"]),
                               params=dict(replica="aws"))
            print(res)
            print(res.json())
            res.raise_for_status()
            assert res.json()["contents"] == contents
        res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                           headers=dict(Authorization="Bearer "+os.environ["GOOG_AUTH_TOKEN"]),
                           params=dict(replica="aws", version="9000"))
        print(res)
        assert res.status_code == requests.codes.not_found
        res = requests.patch("http://localhost:5000/v1/collections/{}".format(uuid),
                             headers=dict(Authorization="Bearer "+os.environ["GOOG_AUTH_TOKEN"]),
                             params=dict(replica="aws"),
                             json=dict())
        assert res.status_code == requests.codes.bad_request
        for patch_payload in [dict(),
                              dict(description="foo", name="cn"),
                              dict(description="bar", details={1: 2}),
                              dict(addContents=contents),
                              dict(removeContents=contents)]:
            with self.subTest("Patch with {}".format(patch_payload)):
                res = requests.patch("http://localhost:5000/v1/collections/{}".format(uuid),
                                     headers=dict(Authorization="Bearer "+os.environ["GOOG_AUTH_TOKEN"]),
                                     params=dict(version=version, replica="aws"),
                                     json=patch_payload)
                print(res)
                print(res.json())
                res.raise_for_status()
                version = res.json()["version"]
        with self.subTest("Get updated version of collection"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=dict(Authorization="Bearer " + os.environ["GOOG_AUTH_TOKEN"]),
                               params=dict(replica="aws"))
            print(res)
            print(res.json())
            res.raise_for_status()
            assert res.json() == dict(contents=[], description='bar', details={"1": 2}, name='cn')
        with self.subTest("Delete collection"):
            res = requests.delete("http://localhost:5000/v1/collections/{}".format(uuid),
                                  headers=dict(Authorization="Bearer " + os.environ["GOOG_AUTH_TOKEN"]),
                                  params=dict(replica="aws"))
            print(res)
            print(res.json())
            res.raise_for_status()

if __name__ == '__main__':
    unittest.main()

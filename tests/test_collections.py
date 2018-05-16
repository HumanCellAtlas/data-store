#!/usr/bin/env python3

import datetime
import os
import unittest
from uuid import uuid4
from botocore.vendored import requests


class TestCollections(unittest.TestCase):
    def test_collections(self):
        contents = [dict(type="file", uuid="0133c91a-31ac-43b8-9819-84903289064d", version="2018-03-23T164152.102044Z")] * 20
        with self.subTest("Create new collection"):
            res = requests.put("http://localhost:5000/v1/collections",
                               headers=dict(Authorization="Bearer "+os.environ["GOOG_AUTH_TOKEN"]),
                               params=dict(uuid=str(uuid4()), version=datetime.datetime.now().isoformat(), replica="aws"),
                               json=dict(name="n", description="d", details={}, contents=contents))
            res.raise_for_status()
            uuid, version = res.json()["uuid"], res.json()["version"]
        with self.subTest("Get created collection"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=dict(Authorization="Bearer "+os.environ["GOOG_AUTH_TOKEN"]),
                               params=dict(version=version, replica="aws"))
            res.raise_for_status()
            self.assertEqual(res.json()["contents"], contents)
        with self.subTest("Get latest version of collection"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=dict(Authorization="Bearer "+os.environ["GOOG_AUTH_TOKEN"]),
                               params=dict(replica="aws"))
            res.raise_for_status()
            self.assertEqual(res.json()["contents"], contents)
        res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                           headers=dict(Authorization="Bearer "+os.environ["GOOG_AUTH_TOKEN"]),
                           params=dict(replica="aws", version="9000"))
        self.assertEqual(res.status_code, requests.codes.not_found)
        res = requests.patch("http://localhost:5000/v1/collections/{}".format(uuid),
                             headers=dict(Authorization="Bearer "+os.environ["GOOG_AUTH_TOKEN"]),
                             params=dict(replica="aws"),
                             json=dict())
        self.assertEqual(res.status_code, requests.codes.bad_request)
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
                res.raise_for_status()
                version = res.json()["version"]
        with self.subTest("Get updated version of collection"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=dict(Authorization="Bearer " + os.environ["GOOG_AUTH_TOKEN"]),
                               params=dict(replica="aws"))
            res.raise_for_status()
            collection = res.json()
            del collection["owner"]
            self.assertEqual(collection,
                             dict(contents=[], description='bar', details={"1": 2}, name='cn'))
        with self.subTest("Delete collection"):
            res = requests.delete("http://localhost:5000/v1/collections/{}".format(uuid),
                                  headers=dict(Authorization="Bearer " + os.environ["GOOG_AUTH_TOKEN"]),
                                  params=dict(replica="aws"))
            res.raise_for_status()
        with self.subTest("Verify deleted"):
            res = requests.get("http://localhost:5000/v1/collections/{}".format(uuid),
                               headers=dict(Authorization="Bearer " + os.environ["GOOG_AUTH_TOKEN"]),
                               params=dict(replica="aws"))
            self.assertEqual(res.status_code, requests.codes.not_found)
        with self.subTest("Invalid fragment reference"):
            pass
        with self.subTest("Dedup semantics"):
            pass
        with self.subTest("Read access control"):
            pass

if __name__ == '__main__':
    unittest.main()

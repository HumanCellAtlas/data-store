#!/usr/bin/env python
# coding: utf-8

import os
import sys
import time
import unittest
from uuid import uuid4

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import DSSAssertMixin, DSSUploadMixin, TestAuthMixin
from tests.infra.server import ThreadedLocalServer
from tests import get_auth_header


class TestProfileBundle(unittest.TestCase, TestAuthMixin, DSSAssertMixin, DSSUploadMixin):
    @classmethod
    def setUpClass(cls):
        cls.uuid = "90484beb-4f21-4626-bf97-7439eee1aea1"
        cls.version = "2019-04-04T170007.204366Z"
        cls.patch_size = 5000
        cls.app = ThreadedLocalServer()
        cls.app.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def test_profile(self):
        resp = dict(uuid=self.uuid, version=self.version)
        for i, payload in enumerate(self.patch_payloads()):
            start_time = time.time()
            resp = self.patch_bundle(self.uuid, resp['version'], payload)
            duration = time.time() - start_time
            print(self.uuid, resp['version'], duration)

    def patch_bundle(self, bundle_uuid, bundle_version, patch_payload):
        resp = self.app.patch(
            f"/v1/bundles/{bundle_uuid}",
            headers=get_auth_header(authorized=True),
            params=dict(version=bundle_version, replica="aws"),
            json=patch_payload
        )
        return resp.json()

    def patch_payloads(self):
        keys = [
            "0185a926-1b58-47fa-a4bd-acceb090ef5c.2018-11-12T235834.225377Z",
            "039b1f97-850a-4e10-985c-31677bb04b11.2018-11-12T235834.225377Z",
            "17c93feb-45ec-4d8f-bb94-d4d35bf2f7a1.2018-11-12T235834.225377Z",
            "4ab87882-2760-4b93-b6cd-0fc80e3da8fd.2018-11-12T235834.225377Z",
            "4ed6abd5-08e3-4f80-925f-fb7fe7c5ac1a.2018-11-12T235854.981860Z",
            "689cb552-d86a-4d0d-ae19-4ed77174c486.2018-11-12T235854.981860Z",
            "6b30dd8a-6602-4ecf-9fc6-2fe4c9c3276c.2018-11-12T235854.981860Z",
            "6dba9fc6-74b0-4997-8caa-06e98abbb75b.2018-11-12T235854.981860Z",
            "741b852c-4bba-47e7-b265-8f977f280ef6.2018-11-12T235854.981860Z",
            "863f2695-351d-429f-97a5-635ac83cc506.2018-11-12T235834.225377Z",
            "92474d62-8b21-4a5b-94d6-7b2e06376049.2018-11-12T235854.981860Z",
            "969ac309-c345-47d8-9e8b-69353e53a968.2018-11-12T235854.981860Z",
            "975c44b9-b689-4d89-85e3-d034d7b9e5bb.2018-11-12T235834.225377Z",
            "98551dc3-ae16-4646-b388-44c2477fad3f.2018-11-12T235834.225377Z",
            "98e9e3b0-e2b3-4a48-9814-641e75bb8c18.2018-11-12T235854.981860Z",
            "edd8fe1c-3edc-468f-bd72-5ea41b42b80c.2018-11-12T235834.225377Z",
        ]
        i = 0
        items = list()
        while True:
            fqid = keys[i]
            i = (i + 1) % len(keys)
            uuid, version = fqid.split(".", 1)
            items.append({
                "indexed": False,
                "name": str(uuid4()),
                "uuid": uuid,
                "version": version,
            })
            if self.patch_size == len(items):
                yield dict(add_files=items)
                items = list()

if __name__ == '__main__':
    unittest.main()

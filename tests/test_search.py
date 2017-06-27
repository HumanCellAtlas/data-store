#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import json
import os
import sys
import unittest

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # noqa
sys.path.insert(0, pkg_root) # noqa

import dss
from tests.infra import DSSAsserts


class TestSearch(unittest.TestCase, DSSAsserts):
    def setUp(self):
        DSSAsserts.setup(self)
        self.app = dss.create_app().app.test_client()

    def test_search_get(self):
        self.assertGetResponse(
            '/v1/search',
            query_string=dict(query=json.dumps(dict(foo=1))),
            expected_code=requests.codes.ok)

    def test_search_post(self):
        query = \
            {
                "query": {
                    "bool": {
                        "must": [{
                            "match": {
                                "files.sample_json.donor.species": "Homo sapiens"
                            }
                        }, {
                            "match": {
                                "files.assay_json.single_cell.method": "Fluidigm C1"
                            }
                        }, {
                            "match": {
                                "files.sample_json.ncbi_biosample": "SAMN04303778"
                            }
                        }]
                    }
                }
            }
        self.assertPostResponse(
            '/v1/search',
            json_request_body=(query),
            expected_code=requests.codes.ok)

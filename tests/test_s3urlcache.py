#!/usr/bin/env python
# coding: utf-8

import logging
import os
import io
import shutil
import sys
import unittest
import threading
import requests
from http.server import BaseHTTPRequestHandler, HTTPServer

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Config, BucketConfig
from dss.config import Replica
from dss.logging import configure_test_logging
from dss.util import networking
from dss.util.s3urlcache import S3UrlCache, SizeLimitError
from tests.infra import testmode

logger = logging.getLogger(__name__)

KiB = 1024
MB = KiB ** 2

randomdata = 'a random string of data'

class HTTPInfo:
    address = "127.0.0.1"
    port = None
    server = None
    thread = None

    @classmethod
    def make_url(cls):
        cls.url = f"http://{cls.address}:{cls.port}"


def setUpModule():
    configure_test_logging()
    HTTPInfo.port = networking.unused_tcp_port()
    HTTPInfo.server = HTTPServer((HTTPInfo.address, HTTPInfo.port), GetTestHandler)
    HTTPInfo.make_url()
    HTTPInfo.thread = threading.Thread(target=HTTPInfo.server.serve_forever)
    HTTPInfo.thread.start()

    global randomdata
    randomdata = os.urandom(MB)


def tearDownModule():
    HTTPInfo.server.shutdown()

@testmode.standalone
class TestS3UrlCache(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        replica = Replica.aws
        Config.set_config(BucketConfig.TEST_FIXTURE)
        cls.blobstore = Config.get_blobstore_handle(replica)
        cls.test_fixture_bucket = replica.bucket
        Config.set_config(BucketConfig.TEST)
        cls.test_bucket = replica.bucket

    def setUp(self):
        self.urls_to_cleanup = set()
        self.cache = S3UrlCache()

    def tearDown(self):
        self._delete_cached_urls()

    def test_store_in_cache(self):
        """The URL contents are stored in S3 and the contents returned, when requested url is not found in cache."""
        url = f"{HTTPInfo.url}/{KiB}"
        self.urls_to_cleanup.add(url)
        url_key = S3UrlCache._url_to_key(url)

        self._delete_cached_urls()
        with self.assertLogs(dss.logger, "INFO") as log_monitor:
            url_content = self.cache.resolve(url)

        original_data = randomdata[:KiB]
        self.assertEqual(len(url_content), KiB)
        self.assertEqual(url_content, original_data)
        self.assertTrue(log_monitor.output[0].endswith(f"{url} not found in cache. Adding it to "
                                                       f"{self.test_bucket} with key {url_key}."))

    def test_retrieve_from_cache(self):
        """Stored URL contents is retrieved from S3, when a cached url is requested"""
        url = f"{HTTPInfo.url}/{KiB}"
        self.urls_to_cleanup.add(url)
        with self.assertLogs(dss.logger, 'INFO') as log_monitor:
            url_content = self.cache.resolve(url)
            cached_content = self.cache.resolve(url)
        self.assertEqual(len(log_monitor.output), 1)
        self.assertEqual(url_content, cached_content)

    def test_bad_url(self):
        bad_urls = ['', '//', 'http://?']
        self.urls_to_cleanup.update(bad_urls)

        for url in bad_urls:
            with self.subTest(bad_url=url):
                with self.assertRaises(requests.RequestException):
                    self.cache.resolve(url)

    def test_url_SizeLimitError(self):
        """Exception returned when URL content size is greater than max_size."""
        url = f"{HTTPInfo.url}/{KiB}"
        self.urls_to_cleanup.add(url)
        self.cache.max_size = 1
        with self.assertRaises(SizeLimitError) as ex:
            self.cache.resolve(url)
        self.assertEqual(ex.exception.args[0], f"{url} not cached. The URL's contents have exceeded "
                                               f"{self.cache.max_size} bytes.")

    def test_content_size_0(self):
        url = f"{HTTPInfo.url}/0"
        self.urls_to_cleanup.add(url)
        url_content = self.cache.resolve(url)
        self.assertEqual(len(url_content), 0)

    def test_chunked_content(self):
        size = KiB * 10
        url = f"{HTTPInfo.url}/{size}"
        self.urls_to_cleanup.add(url)
        self.cache.max_size = 1 * MB
        self.cache.chunk_size = 1 * KiB
        url_content = self.cache.resolve(url)
        original_data = randomdata[:size]
        self.assertEqual(url_content, original_data)

    def test_stored_url_metadata(self):
        url = f"{HTTPInfo.url}/{KiB}"
        self.urls_to_cleanup.add(url)
        url_key = S3UrlCache._url_to_key(url)
        self.cache.resolve(url)

        with self.subTest("check dss_cached_url"):
            cached_url = self.cache._reverse_key_lookup(url_key)
            self.assertEqual(cached_url, url)

        with self.subTest("check content_type"):
            contentType = self.blobstore.get_content_type(self.test_bucket, url_key)
            self.assertEqual(contentType, "application/octet-stream")

    def test_evict(self):
        url = f"{HTTPInfo.url}/{KiB}"
        self.urls_to_cleanup.add(url)
        url_key = S3UrlCache._url_to_key(url)

        with self.assertLogs(dss.logger, "INFO") as log_monitor:
            # Verify the URL is cached
            self.cache.resolve(url)
            self.assertTrue(self.cache.contains(url))
            # Remove the URL from cache
            self.cache.evict(url)
            self.assertTrue(not self.cache.contains(url))
            self.cache.evict(url)
            self.assertTrue(not self.cache.contains(url))

        self.assertTrue(log_monitor.output[0].endswith(f"{url} not found in cache. Adding it to "
                                                       f"{self.test_bucket} with key {url_key}."))
        self.assertTrue(log_monitor.output[1].endswith(f"{url} removed from cache in {self.test_bucket}."))
        self.assertTrue(log_monitor.output[2].endswith(f"{url} not found and not removed from cache."))

    def test_contains(self):
        url = f"{HTTPInfo.url}/{KiB}"
        self.urls_to_cleanup.add(url)

        self.assertTrue(not self.cache.contains(url))
        self.cache.resolve(url)
        self.assertTrue(self.cache.contains(url))

    def _delete_cached_urls(self):
        for url in self.urls_to_cleanup:
            self.cache.evict(url)


class GetTestHandler(BaseHTTPRequestHandler):
    _response_code = 200

    def do_GET(self):
        length = int(self.path.split('/')[1])

        self.send_response(self._response_code)
        self.send_header("content-length", length)
        self.end_headers()

        if length:
            shutil.copyfileobj(io.BytesIO(randomdata[:length]), self.wfile)
        else:
            return

    def log_request(self, code='-', size='-'):
        if Config.debug_level():
            super().log_request(code, size)


if __name__ == "__main__":
    unittest.main()

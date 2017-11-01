#!/usr/bin/env python
# coding: utf-8

"""
Test that the standalone can start up and answer a request.
"""

import os
import socket
import subprocess
import sys
import time
import unittest
import uuid

import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util import networking


class TestStandaloneScript(unittest.TestCase):
    @classmethod
    def setUpClass(cls, timeout_seconds=10):
        dss_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        cls.port = networking.unused_tcp_port()
        cls.subprocess = subprocess.Popen(
            [
                os.path.join(dss_root_path, "dss-api"),
                "--port",
                str(cls.port),
                "--log-level",
                "CRITICAL",
            ],
            cwd=dss_root_path
        )

        end_time = time.time() + timeout_seconds
        delay = 0.05
        while time.time() < end_time:
            try:
                socket.create_connection(("localhost", cls.port))
                break
            except ConnectionError:
                delay = max(1.0, delay * 2)
                time.sleep(delay)
                continue

    @classmethod
    def tearDownClass(cls):
        cls.subprocess.terminate()
        cls.subprocess.wait()

    def test_simple_request(self):
        file_uuid = str(uuid.uuid4())
        response = requests.api.get(f"http://localhost:{self.port}/v1/files/{file_uuid}?replica=aws")
        self.assertEqual(response.status_code, requests.codes.not_found)


if __name__ == '__main__':
    unittest.main()

import chalice.config
import functools
import os
import sys
import signal
import types
import requests
import cgi
import json
import subprocess
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from chalice.cli import CLIFactory
from chalice.local import LocalDevServer, ChaliceRequestHandler
import socketserver

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import Config, BucketConfig


def start_multiprocess_mock_fusillade_server():
    """Use Popen to start the mock fusillade server and stash the PID in a file"""
    pid_file = MockFusilladeServer.get_pid_filename()

    # If there is a PID file, verify process still running or delete PID file
    if os.path.isfile(pid_file):
        try:
            with open(pid_file, 'r') as f:
                pid = int(f.read())
            # Does not kill process, just prompts process for status
            os.kill(pid, 0)
        except (ValueError, OSError):
            subprocess.call(['rm', '-f', pid_file])

    # If there is no PID file, start the mock fusillade server
    if not os.path.isfile(pid_file):
        # Start a new mock fusillade server process
        cmd = os.path.join(pkg_root, "tests", "infra", "mock_fusillade_start.py")
        p = subprocess.Popen([cmd])
        # Write pid to file
        with open(pid_file, "w") as f:
            f.write(str(p.pid))

    # Let the server start
    time.sleep(3)
    return


class MockFusilladeServer(BaseHTTPRequestHandler):
    """
    Create a mock Fusillade auth server endpoint so that any operation that tries to check
    permissions with Fusillade will be handled correctly; we keep it simple and accept/reject
    based on whether the principal (user) is on the whitelist or not.
    """
    _address = "127.0.0.1"
    _port = None
    _server = None
    _request = None
    _whitelist = [
        "valid@ucsc.edu",
        "travis-test@human-cell-atlas-travis-test.iam.gserviceaccount.com",
        "org-humancellatlas-integration@human-cell-atlas-travis-test.iam.gserviceaccount.com",
    ]

    @classmethod
    def startServing(cls):
        Config.set_config(BucketConfig.TEST)
        cls.stash_oidc_group_claim()
        cls.stash_openid_provider()
        cls._port = cls.get_port()
        # Allow multiple servers to be created/destroyed during tests
        HTTPServer.allow_reuse_address = True
        cls._server = HTTPServer((cls._address, cls._port), cls)
        cls._server.serve_forever()
        signal.signal(signal.SIGTERM, cls.clean_up)

    @classmethod
    def get_pid_filename(cls):
        """Get filename where PID is stored"""
        return os.path.join(pkg_root, ".mock_fusillade_pid")

    @classmethod
    def clean_up(cls):
        """Remove the PID file"""
        pid_file = cls.get_pid_filename()
        rm_cmd = ['rm', '-f', pid_file]
        subprocess.call(rm_cmd)

    @classmethod
    def get_port(cls):
        authz_url = Config.get_authz_url()
        split_authz_url = authz_url.split(":")
        if len(split_authz_url) == 3:
            return int(split_authz_url[-1])
        else:
            raise RuntimeError(
                f"Error: AuthZ URL {authz_url} is malformed for tests, need a port number.\n "
                "Check test configuration values and dss/config.py get_authz_url()."
            )

    @classmethod
    def get_endpoint(cls):
        cls._port = cls.get_port()
        endpoint = f"http://{cls._address}:{cls._port}"
        return endpoint

    @classmethod
    def stash_oidc_group_claim(cls):
        """Stash the OIDC_GROUP_CLAIM env var and replace it with a test value"""
        key = "OIDC_GROUP_CLAIM"
        cls._old_oidc_group_claim = os.environ.pop(key, "EMPTY")
        os.environ[key] = "https://auth.data.humancellatlas.org/group"

    @classmethod
    def restore_oidc_group_claim(cls):
        """Restore the OIDC_GROUP_CLAIM env var when mock fusillade server is done"""
        key = "OIDC_GROUP_CLAIM"
        os.environ[key] = cls._old_oidc_group_claim

    @classmethod
    def stash_openid_provider(cls):
        """Stash the OPENID_PROVIDER env var and replace it with a test value"""
        key = "OPENID_PROVIDER"
        cls._old_openid_provider = os.environ.pop(key, "EMPTY")
        os.environ[key] = "https://humancellatlas.auth0.com/"

    @classmethod
    def restore_openid_provider(cls):
        """Restore the OPENID_PROVIDER env var when mock fusillade server is done"""
        key = "OPENID_PROVIDER"
        os.environ[key] = cls._old_openid_provider

    def _set_headers(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    def do_POST(self):
        ctype, pdict = cgi.parse_header(self.headers.get("content-type"))
        # Enforce rule: JSON only
        if ctype != "application/json":
            self.send_response(400)
            self.end_headers()
            return
        # Convert received JSON to dict
        length = int(self.headers.get("content-length"))
        message = json.loads(self.rfile.read(length))
        # Only allow if principal is on whitelist
        if message["principal"] in self._whitelist:
            message["result"] = True
        else:
            message["result"] = False
        # Send it back
        self._set_headers()
        self.wfile.write(bytes(json.dumps(message), "utf8"))

    # def log_request(self, *args, **kwargs):
    #     """
    #     If this empty method is defined, it overrides the (otherwise noisy) log messages
    #     from the HTTP server as it starts, stops, and receives requests.
    #     """
    #     # Quiet plz
    #     pass


if __name__ == "__main__":
    MockFusilladeServer.startServing()

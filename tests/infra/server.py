import chalice.config
import functools
import os
import sys
import threading
import types
import requests
import cgi
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from chalice.cli import CLIFactory
from chalice.local import LocalDevServer, ChaliceRequestHandler

from dss.util import networking
from dss import Config


class ThreadedMockFusilladeServer(BaseHTTPRequestHandler):
    """
    Create a mock Fusillade auth server endpoint so that any operation that tries to check
    permissions with Fusillade will be handled correctly; we keep it simple and accept/reject
    based on whether the principal (user) is on the whitelist or the blacklist.
    """
    _address = "127.0.0.1"
    _port = None
    _server = None
    _thread = None
    _request = None
    _whitelist = ['valid@ucsc.edu', 'travis-test@human-cell-atlas-travis-test.iam.gserviceaccount.com']
    _blacklist = ['invalid@ucsc.edu']

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
    def startServing(cls):
        cls._port = cls.get_port()
        cls._server = HTTPServer((cls._address, cls._port), cls)
        cls._thread = threading.Thread(target=cls._server.serve_forever, daemon=True)
        cls._thread.start()
        cls._request = []

    @classmethod
    def stopServing(cls):
        cls._server.shutdown()
        cls._thread.join()

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_POST(self):
        ctype, pdict = cgi.parse_header(self.headers.get('content-type'))
        # Enforce rule: JSON only
        if ctype != 'application/json':
            self.send_response(400)
            self.end_headers()
            return
        # Convert received JSON to dict
        length = int(self.headers.get('content-length'))
        message = json.loads(self.rfile.read(length))
        # Only allow if principal is on whitelist
        if message['principal'] in self._whitelist:
            message['result'] = True
        else:
            message['result'] = False
        # Send it back
        self._set_headers()
        self.wfile.write(bytes(json.dumps(message), "utf8"))

    def log_request(self, *args, **kwargs):
        # Quiet plz
        pass


class SilentHandler(ChaliceRequestHandler):
    """
    The default Chalice request handler is very chatty.  We don't want that polluting our unit test output, so we
    replace the `log_message` function with something quieter.
    """
    def log_message(self, *args, **kwargs):
        pass


class ThreadedLocalServer(threading.Thread):
    """
    This runs a server on another thread.  It also provides an easy interface to make calls to the server.
    """
    def __init__(self, handler_cls=SilentHandler):
        super().__init__(daemon=True)
        self._port = networking.unused_tcp_port()
        self._server = None
        self._server_ready = threading.Event()
        self._chalice_app = None
        self._handler_cls = handler_cls

    def start(self):
        """
        Start the server and wait for the server to finish loading.
        """
        super().start()
        self._server_ready.wait()

    def run(self):
        project_dir = os.path.join(os.path.dirname(__file__), "..", "..", "chalice")
        factory = CLIFactory(project_dir=project_dir)
        self._chalice_app = factory.load_chalice_app()
        self._chalice_app._override_exptime_seconds = 86400  # something large.  sys.maxsize causes chalice to flip.

        config = chalice.config.Config.create(lambda_timeout=self._chalice_app._override_exptime_seconds)

        self._server = LocalDevServer(self._chalice_app, config, host="", port=self._port,
                                      handler_cls=self._handler_cls)
        self._server_ready.set()
        self._server.server.serve_forever()

    def _make_call(self, method, path, **kwargs):
        result = method(
            f"http://127.0.0.1:{self._port}{path}",
            allow_redirects=False,
            timeout=(1.0, 30.0),
            **kwargs)
        return result

    @classmethod
    def _inject_api_requests_methods(cls):
        """
        requests.api is a module consisting of all the HTTP request types, defined as methods.  If we're being called
        with one of these types, then execute the call against the running server.
        """
        for name in dir(requests.api):
            if not name.startswith('_'):
                func = getattr(requests.api, name)
                if isinstance(func, types.FunctionType) and func.__module__ == requests.api.__name__:
                    setattr(cls, name, functools.partialmethod(cls._make_call, func))

    def shutdown(self):
        if self._server is not None:
            self._server.server.shutdown()
        self.join(timeout=30)
        assert not self.is_alive(), 'Failed to join thread'


# noinspection PyProtectedMember
ThreadedLocalServer._inject_api_requests_methods()

import chalice.config
import logging
import os
import threading
import types

import requests
from chalice.cli import CLIFactory
from chalice.local import LocalDevServer, ChaliceRequestHandler

from dss.util import networking


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
    def __init__(self):
        super().__init__()
        self._port = networking.unused_tcp_port()
        self._server = None
        self._server_ready = threading.Event()
        self._chalice_app = None

    def start(self, *args, **kwargs):
        """
        Start the server and wait for the server to finish loading.
        """
        super().start(*args, **kwargs)
        self._server_ready.wait()

    def run(self):
        project_dir = os.path.join(os.path.dirname(__file__), "..", "..", "chalice")
        factory = CLIFactory(project_dir=project_dir)
        self._chalice_app = factory.load_chalice_app()
        self._chalice_app.log.setLevel(logging.WARNING)
        self._chalice_app._override_exptime_seconds = 86400  # something large.  sys.maxsize causes chalice to flip.

        config = chalice.config.Config.create(lambda_timeout=self._chalice_app._override_exptime_seconds)

        self._server = LocalDevServer(self._chalice_app, config, self._port, handler_cls=SilentHandler)
        self._server_ready.set()
        self._server.server.serve_forever()

    def _make_call(self, method, path, **kwargs):
        return method(
            f"http://localhost:{self._port}{path}",
            allow_redirects=False,
            timeout=(1.0, 30.0),
            **kwargs)

    def __getattr__(self, name):
        """
        requests.api is a module consisting of all the HTTP request types, defined as methods.  If we're being called
        with one of these types, then execute the call against the running server.
        """
        func = getattr(requests.api, name, None)
        if func is not None and isinstance(func, types.FunctionType):
            def result(path, **kwargs):
                return self._make_call(func, path, **kwargs)
            return result

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def shutdown(self):
        if self._server is not None:
            self._server.server.shutdown()

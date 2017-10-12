import contextlib
import logging
import os
import socket
import threading
import types

import requests
from chalice.cli import CLIFactory
from chalice.local import LocalDevServer, ChaliceRequestHandler

from dss import Config, DeploymentStage


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
        self._port = _unused_tcp_port()
        self._server = None
        self._server_ready = threading.Event()

    def start(self, *args, **kwargs):
        """
        Start the server and wait for the server to finish loading.
        """
        super().start(*args, **kwargs)
        self._server_ready.wait()

    def run(self):
        project_dir = os.path.join(os.path.dirname(__file__), "..", "..", "chalice")
        factory = CLIFactory(project_dir=project_dir)
        app = factory.load_chalice_app()
        app.log.setLevel(logging.WARNING)

        config = factory.create_config_obj(
            chalice_stage_name=os.environ["DSS_DEPLOYMENT_STAGE"],
        )

        self._server = LocalDevServer(app, config, self._port, handler_cls=SilentHandler)
        self._server_ready.set()
        self._server.server.serve_forever()

    def _make_call(self, method, path, **kwargs):
        return method(
            f"http://localhost:{self._port}{path}",
            allow_redirects=False,
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


def _unused_tcp_port():
    with contextlib.closing(socket.socket()) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]

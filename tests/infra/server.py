import chalice.config
import functools
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
        return method(
            f"http://127.0.0.1:{self._port}{path}",
            allow_redirects=False,
            timeout=(1.0, 30.0),
            **kwargs)

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

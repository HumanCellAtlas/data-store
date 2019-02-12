import contextlib, socket, threading, logging

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import retry, timeout

logger = logging.getLogger(__name__)

class RetryPolicy(retry.Retry):
    def __init__(self, retry_after_status_codes={301}, *args, **kwargs):
        super(RetryPolicy, self).__init__(*args, **kwargs)
        self.RETRY_AFTER_STATUS_CODES = frozenset(retry_after_status_codes | retry.Retry.RETRY_AFTER_STATUS_CODES)

    def increment(self, *args, **kwargs):
        retry = super(RetryPolicy, self).increment(*args, **kwargs)
        logger.warning("Retrying: {}".format(retry.history[-1]))
        return retry

class HTTPRequest:
    retry_policy = RetryPolicy(read=4,
                               status=4,
                               backoff_factor=0.1,
                               status_forcelist=frozenset({500, 502, 503, 504}))
    timeout_policy = timeout.Timeout(connect=20, read=40)

    def __init__(self):
        self.sessions = {}

    def __call__(self, *args, **kwargs):
        if threading.get_ident() not in self.sessions:
            session = requests.Session()
            adapter = HTTPAdapter(max_retries=self.retry_policy)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self.sessions[threading.get_ident()] = session
        return self.sessions[threading.get_ident()].request(*args, timeout=self.timeout_policy, **kwargs)

request = HTTPRequest()

def unused_tcp_port():
    with contextlib.closing(socket.socket()) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]

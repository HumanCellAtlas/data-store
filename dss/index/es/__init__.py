
import elasticsearch
import logging
import os
import socket
import subprocess
import tempfile
import time
import typing
from urllib.parse import parse_qs, urlencode, urlparse

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.vendored import requests
from elasticsearch import RequestsHttpConnection, Elasticsearch
from requests_aws4auth import AWS4Auth

from dss.util.retry import retry
from dss.util import networking

logger = logging.getLogger(__name__)

class AWSV4Sign(requests.auth.AuthBase):
    """
    AWS V4 Request Signer for Requests.
    """

    def __init__(self, credentials, region, service):
        if not region:
            raise ValueError("You must supply an AWS region")
        self.credentials = credentials
        self.region = region
        self.service = service

    def __call__(self, r):
        url = urlparse(r.url)
        path = url.path or '/'
        querystring = ''
        if url.query:
            querystring = '?' + urlencode(parse_qs(url.query), doseq=True)
        safe_url = url.scheme + '://' + url.netloc.split(':')[0] + path + querystring
        request = AWSRequest(method=r.method.upper(), url=safe_url, data=r.body)
        SigV4Auth(self.credentials, self.service, self.region).add_auth(request)
        r.headers.update(dict(request.headers.items()))
        return r


class ElasticsearchServer:
    def __init__(self, startup_timeout_seconds: int=60) -> None:
        elasticsearch_binary = os.getenv("DSS_TEST_ES_PATH", "elasticsearch")
        self.tempdir = tempfile.TemporaryDirectory()

        while True:
            port = networking.unused_tcp_port()
            transport_port = networking.unused_tcp_port()

            # Set Elasticsearch's initial and max heap to 1.6 GiB, 40% of what's available on Travis, according to
            # guidance from https://www.elastic.co/guide/en/elasticsearch/reference/current/heap-size.html
            env = dict(os.environ, ES_JAVA_OPTIONS="-Xms1638m -Xmx1638m")

            # Work around https://github.com/travis-ci/travis-ci/issues/8408
            if '_JAVA_OPTIONS' in env:  # no coverage
                logger.warning("_JAVA_OPTIONS is set. This may override the options just set via ES_JAVA_OPTIONS.")

            args = [elasticsearch_binary,
                    "-E", f"http.port={port}",
                    "-E", f"transport.tcp.port={transport_port}",
                    "-E", f"path.data={self.tempdir.name}",
                    "-E", "logger.org.elasticsearch=warn"]
            logger.debug("Running %r with environment %r", args, env)
            proc = subprocess.Popen(args, env=env)
            for ix in range(startup_timeout_seconds):
                try:
                    sock = socket.create_connection(("127.0.0.1", port), 1)
                    sock.close()
                    break
                except (ConnectionRefusedError, socket.timeout):
                    # failed :(
                    pass
                time.sleep(1)
            else:
                # still not running.  try a different port.
                continue

            # is the process still running?  if not, we're probably talking to someone else.
            if proc.poll() is None:
                self.port = port
                self.proc = proc
                break

    def shutdown(self) -> None:
        self.proc.kill()
        self.proc.communicate()
        self.proc.wait()
        self.tempdir.cleanup()


class ElasticsearchClient:
    _es_client = dict()  # type: typing.MutableMapping[typing.Tuple[str, int], Elasticsearch]

    @staticmethod
    def get() -> Elasticsearch:
        elasticsearch_endpoint = os.getenv("DSS_ES_ENDPOINT", "localhost")
        elasticsearch_port = int(os.getenv("DSS_ES_PORT", "443"))

        client = ElasticsearchClient._es_client.get((elasticsearch_endpoint, elasticsearch_port), None)

        if client is None:
            try:
                logger.debug("Connecting to Elasticsearch at host: {}".format(elasticsearch_endpoint))
                if elasticsearch_endpoint.endswith(".amazonaws.com"):
                    session = boto3.session.Session()
                    # TODO (akislyuk) Identify/resolve why use of AWSV4Sign results in an AWS auth error
                    # when Elasticsearch scroll is used. Work around this by using the
                    # requests_aws4auth package as described here:
                    # https://elasticsearch-py.readthedocs.io/en/master/#running-on-aws-with-iam
                    # es_auth = AWSV4Sign(session.get_credentials(), session.region_name, service="es")
                    # Begin workaround
                    current_credentials = session.get_credentials().get_frozen_credentials()
                    es_auth = AWS4Auth(current_credentials.access_key, current_credentials.secret_key,
                                       session.region_name, "es", session_token=current_credentials.token)
                    # End workaround
                    client = Elasticsearch(
                        hosts=[{'host': elasticsearch_endpoint, 'port': elasticsearch_port}],
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection,
                        http_auth=es_auth)
                else:
                    client = Elasticsearch(
                        [{'host': elasticsearch_endpoint, 'port': elasticsearch_port}],
                        use_ssl=False
                    )
                ElasticsearchClient._es_client[(elasticsearch_endpoint, elasticsearch_port)] = client
            except Exception as ex:
                logger.error(f"Unable to connect to Elasticsearch endpoint {elasticsearch_endpoint}. Exception: {ex}")
                raise ex

        return client


def _retryable_exception(e):
    return (isinstance(e, elasticsearch.TransportError) and
            isinstance(e.status_code, int) and (  # have spotted 'N/A' in the wild
                e.status_code == 409 or  # version conflicts
                500 <= e.status_code <= 599))  # server errors


def _retry_delay(i, delay):
    return 10 if delay is None else delay * 1.5


# noinspection PyPep8Naming
class elasticsearch_retry(retry):
    # noinspection PyShadowingNames
    def __init__(self, logger) -> None:
        super().__init__(timeout=60,  # seconds
                         limit=10,  # retries
                         inherit=True,  # nested retries should obey the outer-most retry's timeout
                         retryable=_retryable_exception,
                         delay=_retry_delay,
                         logger=logger)

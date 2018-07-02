from functools import lru_cache
import logging
import os
from typing import Union
from urllib.parse import parse_qs, urlencode, urlparse

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.vendored import requests
import elasticsearch
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import BulkIndexError, bulk, scan
from requests_aws4auth import AWS4Auth

from dss import Config, ESDocType, ESIndexType, Replica
from dss.util.retry import retry

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


class ElasticsearchClient:
    """
    >>> EC = ElasticsearchClient
    >>> EC.get() is EC.get()
    True
    >>> EC._get("alskdjasfdiw", 443, 1) is EC._get("alskdjasfdiw", 443, 2)
    False
    >>> EC._get("daskdjlsadjl.amazonaws.com", 443, 1) is EC._get("daskdjlsadjl.amazonaws.com", 443, 1)
    True
    """
    @staticmethod
    def get() -> Elasticsearch:
        host = Config.get_elasticsearch_host()
        port = Config.get_elasticsearch_port()
        timeout = Config.get_elasticsearch_timeout()
        return ElasticsearchClient._get(host, port, timeout)

    @staticmethod
    @lru_cache(maxsize=32)
    def _get(host, port, timeout):
        logger.debug("Connecting to Elasticsearch at host: {}".format(host))
        if host.endswith(".amazonaws.com"):
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
            client = Elasticsearch(hosts=[dict(host=host, port=port)],
                                   timeout=timeout,
                                   use_ssl=True,
                                   verify_certs=True,
                                   connection_class=RequestsHttpConnection,
                                   http_auth=es_auth)
        else:
            client = Elasticsearch(hosts=[dict(host=host, port=port)],
                                   timeout=timeout,
                                   use_ssl=False)
        return client


def _retryable_exception(e):
    return (isinstance(e, elasticsearch.TransportError) and
            isinstance(e.status_code, int) and (  # have spotted 'N/A' in the wild
                e.status_code == 409 or  # version conflicts
                500 <= e.status_code <= 599))  # server errors


def _retry_delay(i, delay):
    return .25 if delay is None else delay * 1.5


# noinspection PyPep8Naming
class elasticsearch_retry(retry):
    # noinspection PyShadowingNames
    def __init__(self, logger, timeout=None) -> None:
        super().__init__(timeout=timeout,  # seconds
                         limit=10,  # retries
                         inherit=True,  # nested retries should obey the outer-most retry's timeout
                         retryable=_retryable_exception,
                         delay=_retry_delay,
                         logger=logger)


def refresh_percolate_queries(replica: Replica, index_name: str):
    # When dynamic templates are used and queries for percolation have been added
    # to an index before the index contains mappings of fields referenced by those queries,
    # the queries must be reloaded when the mappings are present for the queries to match.
    # See: https://github.com/elastic/elasticsearch/issues/5750
    subscription_index_name = Config.get_es_index_name(ESIndexType.subscriptions, replica)
    es_client = ElasticsearchClient.get()
    if not es_client.indices.exists(subscription_index_name):
        return
    subscription_queries = [{'_index': index_name,
                             '_type': ESDocType.query.name,
                             '_id': hit['_id'],
                             '_source': hit['_source']['es_query']
                             }
                            for hit in scan(es_client,
                                            index=subscription_index_name,
                                            doc_type=ESDocType.subscription.name,
                                            query={'query': {'match_all': {}}})
                            ]

    if subscription_queries:
        try:
            bulk(es_client, iter(subscription_queries), refresh=True)
        except BulkIndexError as ex:
            logger.error(f"Error occurred when adding subscription queries "
                         f"to index {index_name} Errors: {ex.errors}")

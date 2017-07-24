import json
import os
from urllib.parse import SplitResult, parse_qs, urlencode, urlparse, urlunsplit

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.vendored import requests
from elasticsearch import RequestsHttpConnection, Elasticsearch


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
    _es_client = None

    @staticmethod
    def get(logger):
        if ElasticsearchClient._es_client is None:
            elasticsearch_endpoint = os.getenv("DSS_ES_ENDPOINT")
            try:
                if elasticsearch_endpoint is None:
                    elasticsearch_endpoint = "localhost"
                logger.debug("Connecting to Elasticsearch at host: {}".format(elasticsearch_endpoint))
                if elasticsearch_endpoint.endswith(".amazonaws.com"):
                    session = boto3.session.Session()
                    es_auth = AWSV4Sign(session.get_credentials(), session.region_name, service="es")
                    es_client = Elasticsearch(
                        hosts=[{'host': elasticsearch_endpoint, 'port': 443}],
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection,
                        http_auth=es_auth)
                else:
                    es_client = Elasticsearch([elasticsearch_endpoint], use_ssl=False, port=9200)
                ElasticsearchClient._es_client = es_client
            except Exception as ex:
                logger.error("Unable to connect to Elasticsearch endpoint {}. Exception: {}".format(
                    elasticsearch_endpoint, ex)
                )
                raise ex

        return ElasticsearchClient._es_client


def get_elasticsearch_index(es_client, idx, logger, index_mapping=None):
    try:
        response = es_client.indices.exists(idx)
        if response:
            logger.debug("Using existing Elasticsearch index: {}".format(idx))
        else:
            logger.debug("Creating new Elasticsearch index: {}".format(idx))
            response = es_client.indices.create(idx, body=index_mapping)
            logger.debug("Index creation response: {}", (json.dumps(response, indent=4)))

    except Exception as ex:
        logger.error("Unable to create index {} Exception: {}".format(idx))
        raise ex

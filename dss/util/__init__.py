from urllib.parse import SplitResult, parse_qs, urlencode, urlparse, urlunsplit

import boto3
import typing
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.vendored import requests
from elasticsearch import RequestsHttpConnection, Elasticsearch

from ..hcablobstore import BundleFileMetadata


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


def create_blob_key(file_info: typing.Dict[str, str]) -> str:
    return "blobs/" + ".".join((
        file_info[BundleFileMetadata.SHA256],
        file_info[BundleFileMetadata.SHA1],
        file_info[BundleFileMetadata.S3_ETAG],
        file_info[BundleFileMetadata.CRC32C]
    ))


def paginate(boto3_paginator, *args, **kwargs):
    for page in boto3_paginator.paginate(*args, **kwargs):
        for result_key in boto3_paginator.result_keys:
            for value in page.get(result_key.parsed.get("value"), []):
                yield value


def connect_elasticsearch(elasticsearch_endpoint, logger) -> Elasticsearch:
    try:
        if elasticsearch_endpoint is None:
            elasticsearch_endpoint = "localhost"
        logger.debug("Connecting to Elasticsearch at host: %s", elasticsearch_endpoint)
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
        return es_client
    except Exception as ex:
        logger.error("Unable to connect to Elasticsearch endpoint %s. Exception: %s", elasticsearch_endpoint, ex)
        raise ex


class UrlBuilder:
    def __init__(self):
        self.splitted = SplitResult("", "", "", "", "")
        self.query = list()

    def set(self, scheme: str=None, netloc: str=None, path: str=None, fragment: str=None) -> "UrlBuilder":
        kwargs = dict()
        if scheme is not None:
            kwargs['scheme'] = scheme
        if netloc is not None:
            kwargs['netloc'] = netloc
        if path is not None:
            kwargs['path'] = path
        if fragment is not None:
            kwargs['fragment'] = fragment
        self.splitted = self.splitted._replace(**kwargs)

        return self

    def add_query(self, query_name: str, query_value: str) -> "UrlBuilder":
        self.query.append((query_name, query_value))

        return self

    def __str__(self) -> str:
        result = self.splitted._replace(query=urlencode(self.query, doseq=True))

        return urlunsplit(result)

#!/usr/bin/env python3.6
import argparse
import logging
from urllib.parse import urlparse, urlencode, parse_qs

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.vendored import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)

#
# Delete an index from an AWS Elasticsearch domain.
#
# arguments:
#   -h, --help            show this help message and exit
#   -d DOMAINNAME, --domainname DOMAINNAME
#                         Elasticsearch domain name
#   -i INDEX, --index INDEX
#                         Name of index to delete or "_all"
#

# This class is copied from: https://github.com/jmenga/requests-aws-sign on 6/16/17
# Consider refactoring this into a common utility package if there is need to use this elsewhere.
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


def connect_es_client(ess_domain: str):
    try:
        ess = boto3.client('es')
        ess_domain_status = ess.describe_elasticsearch_domain(DomainName=ess_domain)["DomainStatus"]
        ess_endpoint = ess_domain_status["Endpoint"]

        credentials = boto3.session.Session().get_credentials()
        es_auth = AWSV4Sign(credentials, ess.meta.region_name, service="es")
        es_client = Elasticsearch([ess_endpoint], use_ssl=True, port=443, connection_class=RequestsHttpConnection,
                           http_auth=es_auth)
        return es_client
    except Exception as e:
        log.error("Error connecting to Elasticsearch domain: %s", e)
        exit(1)

def delete_index(es_client: Elasticsearch, index_name: str):
    try:
        es_client.indices.delete(index=index_name, ignore=[400, 404])
    except Exception as e:
        log.error("Error occurred while deleting the index \"%s\": %s", index_name, e)
        exit(2)

def main():
    parser = argparse.ArgumentParser(description="Delete an index from an AWS Elasticsearch domain.")
    parser.add_argument("-d", "--domainname",
                        help="Elasticsearch domain name",
                        required=True)
    parser.add_argument("-i", "--index",
                        help="Name of index to delete or \"_all\"",
                        required=True)
    args = parser.parse_args()

    es_client = connect_es_client(args.domainname)
    delete_index(es_client, args.index)

main()
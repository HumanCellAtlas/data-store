#!/usr/bin/env python
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import Search
from requests_aws4auth import AWS4Auth

_host = "search-dss-index-integration-vx3ftz6xjooc2vr3ayd7qeyh3m.us-east-1.es.amazonaws.com"

def get_es_client(host, port=443):
    session = boto3.session.Session()
    current_credentials = session.get_credentials().get_frozen_credentials()
    es_auth = AWS4Auth(current_credentials.access_key,
                       current_credentials.secret_key,
                       session.region_name,
                       "es",
                       session_token=current_credentials.token)
    return Elasticsearch(hosts=[dict(host=host, port=port)],
                         timeout=10,
                         use_ssl=True,
                         verify_certs=True,
                         connection_class=RequestsHttpConnection,
                         http_auth=es_auth)

def get_by_id(es, doc_id, doc_type=None):
    kwargs = dict(using=es, index="_all")
    if doc_type is not None:
        kwargs['doc_type'] = doc_type
    for hit in Search(**kwargs).query({"terms": {"_id": [doc_id]}}).scan():
        yield hit

client = get_es_client(_host)

for foo in get_by_id(client, "29b6292e-0670-45cb-a420-ff908b7c7a51.2019-12-04T031008.463093Z"):
    print(foo)

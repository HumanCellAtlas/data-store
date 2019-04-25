#! /usr/bin/env python

import os
import sys
import boto3
import functools
from requests_aws4auth import AWS4Auth
from concurrent.futures import ThreadPoolExecutor, as_completed

from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import TransportError


DEPLOYMENT = "integration"


s3 = boto3.client("s3")


@functools.lru_cache()
def get_es_client():
    domain_name = "dss-index-" + DEPLOYMENT
    host = boto3.client("es").describe_elasticsearch_domain(DomainName=domain_name)['DomainStatus']['Endpoint']
    port = 443
    session = boto3.session.Session()
    current_credentials = session.get_credentials().get_frozen_credentials()
    es_auth = AWS4Auth(
        current_credentials.access_key, current_credentials.secret_key,
        session.region_name, "es", session_token=current_credentials.token
    )
    es_client = Elasticsearch(
        hosts=[dict(host=host, port=port)],
        timeout=10,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        http_auth=es_auth
    )
    return es_client

def get_by_id(doc_id, doc_type=None):
    kwargs = dict(using=get_es_client(), index="_all")
    if doc_type is not None:
        kwargs['doc_type'] = doc_type
    search_obj = Search(**kwargs)
    return [hit.to_dict() for hit in search_obj.query({"terms":{"_id":[doc_id]}})]

def search(q, index="_all", doc_type="doc", max_results=2000000, per_page=500):
    if max_results < per_page:
        max_results = per_page

    def get_pages():
        search_after = None
        kwargs = dict(
            index=index,
            doc_type=doc_type,
            size=per_page,
            body=dict(
                sort=[
                    {"uuid": {"order": "desc"}},
                    {"manifest.version": {"missing": "last", "order": "desc"}}
                ],
                _source=False,
                query=q,
            )
        )
        while True:
            if search_after is not None:
                kwargs['body']['search_after'] = search_after
            page = get_es_client().search(**kwargs)
            yield page
            if not len(page['hits']['hits']) or None in page['hits']['hits'][-1]['sort']:
                break
            else:
                search_after = page['hits']['hits'][-1]['sort']

    count = 0
    for page in get_pages():
        for hit in page['hits']['hits']:
            yield hit['_id']
            count += 1
            if count >= max_results:
                break

if __name__ == "__main__":
    c = get_es_client()
    indices = [index for index in c.indices.get("_all")
               if "doc" in index]
    with ThreadPoolExecutor(max_workers=10) as e:
        futures = list()
        for fqid in search({}, index=indices[3]):
            f = e.submit(
                s3.get_object,
                Bucket=f"org-humancellatlas-dss-{DEPLOYMENT}",
                Key=f"bundles/{fqid}"
            )
            futures.append(f)
        for f in as_completed(futures):
            try:
                print(f.result())
            except Exception as e:
                print(e)

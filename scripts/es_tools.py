#! /usr/bin/env python

import os
import sys
import time
import json
import boto3
import functools
from requests_aws4auth import AWS4Auth

import jmespath
from jmespath.exceptions import JMESPathError
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import TransportError

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Config, Replica
from dss.events.handlers.notify_v2 import build_bundle_metadata_document, should_notify


Config.set_config(dss.BucketConfig.NORMAL)


@functools.lru_cache()
def get_es_client():
    domain_name = "dss-index-" + os.environ['DSS_DEPLOYMENT_STAGE']
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

def deindex(doc_id, doc_type=None):
    kwargs = dict(index="_all", body={"query":{"terms":{"_id":[doc_id]}}})
    if doc_type is not None:
        kwargs['doc_type'] = doc_type
    resp = get_es_client().delete_by_query(**kwargs)
    print(resp)

def get_by_id(doc_id, doc_type=None):
    kwargs = dict(using=get_es_client(), index="_all")
    if doc_type is not None:
        kwargs['doc_type'] = doc_type
    search_obj = Search(**kwargs)
    return [hit.to_dict() for hit in search_obj.query({"terms":{"_id":[doc_id]}})]

def get(index="_all", doc_type=None, owner=None):
    kwargs = dict(using=get_es_client(), index=index)
    if doc_type is not None:
        kwargs['doc_type'] = doc_type
    if owner is not None:
        res = Search(**kwargs).query({'bool': {'must': [{'term': {'owner': owner}}]}})
    else:
        res = Search(**kwargs).scan()
    return [hit for hit in res]

def delete_subscriptions_for_owner(owner):
    subs_aws = get(f"dss-{os.environ['DSS_DEPLOYMENT_STAGE']}-aws-subscriptions", owner=owner)
    subs_gcp = get(f"dss-{os.environ['DSS_DEPLOYMENT_STAGE']}-gcp-subscriptions", owner=owner)
    for s in subs_aws + subs_gcp:
        deindex(s.meta['id'])

def print_subscription_owners():
    for o in set([hit['owner'] for hit in get(doc_type="subscription")]):
        print(o)

def search(q, index="_all", max_results=801, per_page=500):
    if max_results < per_page:
        max_results = per_page
    kwargs = dict(
        index=index,
        size=per_page,
        body=dict(
            sort=[
                {"uuid": {"order": "desc"}},
                {"manifest.version": {"missing": "last", "order": "desc"}}
            ],
            query=q
        )
    )
    fqid = str()
    for _ in range(max_results):
        if fqid == ".".join(kwargs['body'].get('search_after', ())):
            res = get_es_client().search(**kwargs)
            if len(res['hits']['hits']):
                kwargs['body']['search_after'] = res['hits']['hits'][-1]['sort']
                hits = iter(res['hits']['hits'])
            else:
                break
        else:
            fqid = next(hits)['_id'] 
            yield fqid

def print_subscription_query_and_matching_bundles():
    owners = [
        "azul-indexer-integration@human-cell-atlas-travis-test.iam.gserviceaccount.com",
    ]
    for owner in owners:
        for s in get(doc_type="subscription", owner=owner):
            query = s['es_query'].to_dict()['query']
            meta = s.meta.to_dict()
            print(s.to_dict()['owner'], meta['id'], meta['index'])
            print(json.dumps(query))
#            for fqid in search(query):
#                print(fqid)
            print()

class SubscriptionMap:
    def __init__(self, es_subscription_uuid, jmespath_query):
        self.es_subscription_uuid = es_subscription_uuid
        self.jmespath_query = jmespath_query
        es_subs = get_by_id(es_subscription_uuid, doc_type="subscription")
        assert 1 == len(es_subs)
        print(es_subs[0].keys())
        self.es_subscription = es_subs[0]
        self.owner = es_subs[0]['owner']
        self.callback_url = es_subs[0]['callback_url']
        self.es_query = es_subs[0]['es_query']
        print(self)

    def test_subscription(*, replica, es_subscription_uuid, jmespath_subscription):
        for fqid in search(self.es_query['query']):
            key = f"bundles/{fqid}"
            doc = build_bundle_metadata_document(replica, key)
            sn = should_notify(replica, jmespath_subscription, doc, key)
            print(key, sn)
            assert sn

    def __str__(self):
        foo = self.es_subscription.copy()
        foo.update(dict(jmespath_query=self.jmespath_query))
        return json.dumps(foo, indent=4)
        foo = {
            'owner': self.owner,
            'callback_url': self.callback_url,
            'es_subscription_uuid': self.es_subscription_uuid,
            'es_query': self.es_query,
            'jmespath_query': self.jmespath_query,
        }
        return json.dumps(foo, indent=2)

def integration_subscriptions_conversion():
    # SubscriptionMap(
    #     "89bfca93-b877-48ef-995d-c69435e83950",
    #     (
    #         "(files.library_preparation_protocol_json[].library_construction_method[].ontology_label | contains(@, `10X v2 sequencing`))"
    #         "&& (files.library_preparation_protocol_json[].end_bias | contains(@, `3 prime tag`))"
    #         "&& (files.library_preparation_protocol_json[].nucleic_acid_source | contains(@, `single cell`))"
    #         "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id[] | (min(@) == `9606` && max(@) == `9606`)"
    #         "&& files.sequencing_protocol_json[].sequencing_approach.ontology_label | not_null(@, `[]`) | !contains(@, `CITE-seq`)"
    #         "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id | not_null(@, `[]`) | !contains(@, `analysis`)"
    #     )
    # )
    # SubscriptionMap(
    #     "d1b8fc71-3753-43a5-b173-2f292da8154f",
    #     (
    #         "(files.library_preparation_protocol_json[].library_construction_method[].ontology | contains(@, `EFO:0008931`))"
    #         "&& (files.sequencing_protocol_json[].paired_end | [0])"
    #         "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id[] | (min(@) == `9606` && max(@) == `9606`)"
    #         "&& files.sequencing_protocol_json[].sequencing_approach.ontology_label | not_null(@, `[]`) | !contains(@, `CITE-seq`)"
    #         "&& files.analysis_process_json[].process_type.text | not_null(@, `[]`) | !contains(@, `analysis`)"
    #     )
    # )
    SubscriptionMap(
        "eb02b7c7-4afb-4499-8baa-3b4f4fdd114d",
        (
            "event_type==`CREATE`"
            " && files.project_json != `null`"
        )
    )
    SubscriptionMap(
        "4bd8ccea-c396-4a1c-bcad-017aea02a018",
        (
            "event_type==`TOMBSTONE`"
        )
    )

if __name__ == "__main__":
    integration_subscriptions_conversion()
    # find_bundles_with_jmespath_filter_green_2()
    # find_bundles_with_jmespath_filter_orange_1()
    # print_subscription_query_and_matching_bundles()
    # hits = get_by_id("89bfca93-b877-48ef-995d-c69435e83950", doc_type="subscription")
    # assert 1 == len(hits)
    # print(hits[0].keys())

"""
azul-indexer-integration@human-cell-atlas-travis-test.iam.gserviceaccount.com 2e89e38b-7ea4-4664-95ac-b9405fda27ea dss-integration-aws-subscriptions
{"bool": {"must_not": [{"term": {"admin_deleted": true}}], "must": [{"exists": {"field": "files.project_json"}}, {"range": {"manifest.version": {"gte": "2018-11-27"}}}]}}

azul-indexer-integration@human-cell-atlas-travis-test.iam.gserviceaccount.com d4c57487-ab23-4164-966d-576a9221112e dss-integration-aws-subscriptions
{"bool": {"must": [{"term": {"admin_deleted": true}}]}}
"""

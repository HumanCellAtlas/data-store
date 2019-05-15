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


DEPLOYMENT = os.environ['DSS_DEPLOYMENT_STAGE']
DEPLOYMENT = "prod"


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
    subs_aws = get(f"dss-{DEPLOYMENT}-aws-subscriptions", owner=owner)
    subs_gcp = get(f"dss-{DEPLOYMENT}-gcp-subscriptions", owner=owner)
    for s in subs_aws + subs_gcp:
        deindex(s.meta['id'])

def print_subscription_owners():
    for o in set([hit['owner'] for hit in get(doc_type="subscription")]):
        print(o)

def search(q, index="_all", doc_type="doc", max_results=2000000, per_page=200):
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
                #_source=False,
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
            yield hit['_id'], hit['_source']
            count += 1
            if count >= max_results:
                break

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
        self.es_subscription = es_subs[0]
        print(self)

    def test(self, replica: Replica, max_results=1000000000):
        jp_sub = dict(owner="test", replica=replica, uuid="test", jmespath_query=self.jmespath_query)
        count = 0
        for fqid, doc in search(self.es_subscription['es_query']['query'], max_results=max_results):
            # doc = build_bundle_metadata_document(replica, f"bundles/{fqid}")
            count += 1
            if not should_notify(replica, jp_sub, doc, f"bundles/{fqid}"):
                print("JMESPath does not mach ES for", fqid)
        print(f"{count} documents tested")

    def __str__(self):
        data = self.es_subscription.copy()
        data.update(dict(jmespath_query=self.jmespath_query))
        return json.dumps(data, indent=4)

def integration_subscriptions_conversion():
    sm = SubscriptionMap(
        # 10X Subscription
        "89bfca93-b877-48ef-995d-c69435e83950",
        (
            "(files.library_preparation_protocol_json[].library_construction_method[].ontology_label | contains(@, `10X v2 sequencing`))"
            "&& (files.library_preparation_protocol_json[].end_bias | contains(@, `3 prime tag`))"
            "&& (files.library_preparation_protocol_json[].nucleic_acid_source | contains(@, `single cell`))"
            "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id[] | (min(@) == `9606` && max(@) == `9606`)"
            "&& files.sequencing_protocol_json[].sequencing_approach.ontology_label | not_null(@, `[]`) | !contains(@, `CITE-seq`)"
            "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id | not_null(@, `[]`) | !contains(@, `analysis`)"
        )
    )
    sm.test(Replica.gcp)

    sm = SubscriptionMap(
        # SS2 Subscription
        "d1b8fc71-3753-43a5-b173-2f292da8154f",
        (
            "(files.library_preparation_protocol_json[].library_construction_method[].ontology | contains(@, `EFO:0008931`))"
            "&& (files.sequencing_protocol_json[].paired_end | [0])"
            "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id[] | (min(@) == `9606` && max(@) == `9606`)"
            "&& files.sequencing_protocol_json[].sequencing_approach.ontology_label | not_null(@, `[]`) | !contains(@, `CITE-seq`)"
            "&& files.analysis_process_json[].process_type.text | not_null(@, `[]`) | !contains(@, `analysis`)"
        )
    )
    sm.test(Replica.gcp)

    # SubscriptionMap(
    #     "eb02b7c7-4afb-4499-8baa-3b4f4fdd114d",
    #     (
    #         "event_type==`CREATE`"
    #         " && files.project_json != `null`"
    #     )
    # )

    # SubscriptionMap(
    #     "4bd8ccea-c396-4a1c-bcad-017aea02a018",
    #     (
    #         "event_type==`TOMBSTONE`"
    #     )
    # )

if __name__ == "__main__":
    print_subscription_owners()
    # for hit in get(doc_type="subscription"):
    #     if hit['owner'].startswith("b"):
    #         print(json.dumps(hit.to_dict(), indent=2))
    # integration_subscriptions_conversion()
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

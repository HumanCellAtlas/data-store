"""
Tools for maintaining the main Elasticsearch index and subscriptions.
"""
import os
import json
import typing
import argparse
import logging
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from string import hexdigits

import boto3
from elasticsearch_dsl import Search
from dcplib.aws.sqs import SQSMessenger, get_queue_url

from dss import Config, Replica
from dss.operations import dispatch
from dss.api.subscriptions_v1 import _delete_subscription
from dss.index.es import ElasticsearchClient


logger = logging.getLogger(__name__)


elasticsearch = dispatch.target("elasticsearch", help=__doc__)


@elasticsearch.action("index-keys",
                      arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                                 "--keys": dict(default=None, nargs="*", help="keys to index.")})
def index_keys(argv: typing.List[str], args: argparse.Namespace):
    """
    Queue an SQS message to the indexer lambda for each key in `keys`.
    """
    index_queue_url = get_queue_url("dss-index-operation-" + os.environ['DSS_DEPLOYMENT_STAGE'])
    with SQSMessenger(index_queue_url) as sqsm:
        for key in args.keys:
            sqsm.send(json.dumps(dict(replica=args.replica, key=key)))

@elasticsearch.action("index",
                      arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                                 "--prefix": dict(default="", help="UUID prefix to index.")})
def index(argv: typing.List[str], args: argparse.Namespace):
    """
    Queue an SQS message to the indexer lambda for each key in object storage beginning with `bundles/{prefix}`.
    If `prefix` is omitted, send a message for each key in object storage beginning with `bundles/`
    """
    replica = Replica[args.replica]
    handle = Config.get_blobstore_handle(replica)
    index_queue_url = get_queue_url("dss-index-operation-" + os.environ['DSS_DEPLOYMENT_STAGE'])

    def _forward_keys(pfx):
        with SQSMessenger(index_queue_url) as sqsm:
            for key in handle.list(replica.bucket, pfx):
                sqsm.send(json.dumps(dict(replica=args.replica, key=key)))

    hex_chars = set(hexdigits.lower())
    with ThreadPoolExecutor(max_workers=10) as e:
        futures = [e.submit(_forward_keys, f"bundles/{args.prefix}{c}") for c in hex_chars]
        for f in as_completed(futures):
            f.result()

@elasticsearch.action("list-subscriptions", arguments={"--owner": dict(type=str)})
def list_subscriptions(argv: typing.List[str], args: argparse.Namespace):
    """
    List ES subscriptions, optionally filtered by owner
    """
    es = get_es_client()
    for hit in get(es, doc_type="subscription"):
        if args.owner is None or hit['owner'] == args.owner:
            print(hit.meta.id, hit['owner'])

@elasticsearch.action("get-subscriptions", arguments={"--uuids": dict(required=True, nargs="*")})
def get_subscriptions(argv: typing.List[str], args: argparse.Namespace):
    """
    For each uuid, output a subscription and associated percolate queries.
    """
    es = get_es_client()
    lock = Lock()

    def _get(uuid):
        for hit in get_by_id(es, uuid):
            with lock:
                print(hit.meta.to_dict(), hit.to_dict())

    with ThreadPoolExecutor(max_workers=2) as e:
        for uuid in args.uuids:
            e.submit(_get, uuid)

@elasticsearch.action("delete-subscriptions", arguments={"--uuids": dict(required=True, nargs="*")})
def delete_subscriptions(argv: typing.List[str], args: argparse.Namespace):
    """
    For each uuid, delete a subscription and associated percolate queries
    """
    es = get_es_client()
    lock = Lock()

    def _delete(uuid):
        count = es.search(index="_all",
                          doc_type="subscription",
                          body={"query": {"terms": {"_id": [uuid]}}})['hits']['total']
        if count > 0:
            _delete_subscription(es, uuid)
            with lock:
                print(f"Removed subscription {uuid}")
        else:
            with lock:
                print(f"No subscriptions found for {uuid}")

    with ThreadPoolExecutor(max_workers=2) as e:
        futures = [e.submit(_delete, uuid) for uuid in args.uuids]
        for f in as_completed(futures):
            f.result()

@elasticsearch.action("get-by-id", arguments={"--ids": dict(required=True, nargs="*")})
def get_doc_by_id(argv: typing.List[str], args: argparse.Namespace):
    es = get_es_client()
    for id_ in args.ids:
        for hit in get_by_id(es, id_):
            print(hit.to_dict())

@functools.lru_cache()
def get_es_client():
    domain_name = "dss-index-" + os.environ['DSS_DEPLOYMENT_STAGE']
    host = boto3.client("es").describe_elasticsearch_domain(DomainName=domain_name)['DomainStatus']['Endpoint']
    os.environ['DSS_ES_ENDPOINT'] = host
    return ElasticsearchClient.get()

def get_by_id(es, doc_id, doc_type=None):
    kwargs = dict(using=es, index="_all")
    if doc_type is not None:
        kwargs['doc_type'] = doc_type
    for hit in Search(**kwargs).query({"terms": {"_id": [doc_id]}}).scan():
        yield hit

def get(es, index="_all", doc_type=None, owner=None):
    kwargs = dict(using=es, index=index)
    if doc_type is not None:
        kwargs['doc_type'] = doc_type
    if owner is not None:
        search = Search(**kwargs).query({'bool': {'must': [{'term': {'owner': owner}}]}})
    else:
        search = Search(**kwargs)
    for hit in search.scan():
        yield hit

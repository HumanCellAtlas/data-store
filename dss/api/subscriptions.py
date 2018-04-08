import datetime
import logging
from typing import List
from uuid import uuid4

import requests
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ElasticsearchException, NotFoundError
from elasticsearch_dsl import Search
from flask import jsonify, request

from dss import Config, Replica, ESIndexType, ESDocType
from dss.error import DSSException, dss_handler
from dss.index.es import ElasticsearchClient
from dss.index.es.manager import IndexManager


logger = logging.getLogger(__name__)


@dss_handler
def get(uuid: str, replica: str):
    owner = request.token_info['email']

    es_client = ElasticsearchClient.get()

    try:
        response = es_client.get(index=Config.get_es_index_name(ESIndexType.subscriptions, Replica[replica]),
                                 doc_type=ESDocType.subscription.name,
                                 id=uuid)
    except NotFoundError as ex:
        raise DSSException(requests.codes.not_found, "not_found", "Cannot find subscription!")

    source = response['_source']
    source['uuid'] = uuid
    source['replica'] = replica

    if source['owner'] != owner:
        # common_error_handler defaults code to capitalized 'Forbidden' for Werkzeug exception. Keeping consistent.
        raise DSSException(requests.codes.forbidden, "Forbidden", "Your credentials can't access this subscription!")

    return jsonify(source), requests.codes.okay


@dss_handler
def find(replica: str):
    owner = request.token_info['email']
    es_client = ElasticsearchClient.get()

    search_obj = Search(using=es_client,
                        index=Config.get_es_index_name(ESIndexType.subscriptions, Replica[replica]),
                        doc_type=ESDocType.subscription.name)
    search = search_obj.query({'match': {'owner': owner}})

    responses = [{
        'uuid': hit.meta.id,
        'replica': replica,
        'owner': owner,
        'endpoint': hit.endpoint.to_dict(),
        'es_query': hit.es_query.to_dict()}
        for hit in search.scan()]

    full_response = {'subscriptions': responses}
    return jsonify(full_response), requests.codes.okay


@dss_handler
def put(json_request_body: dict, replica: str):
    uuid = str(uuid4())
    es_query = json_request_body['es_query']
    owner = request.token_info['email']

    es_client = ElasticsearchClient.get()

    index_mapping = {
        "mappings": {
            ESDocType.subscription.name: {
                "properties": {
                    "owner": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "es_query": {
                        "type": "object",
                        "enabled": "false"
                    }
                }
            }
        }
    }
    # Elasticsearch preprocesses inputs by splitting strings on punctuation.
    # So for john@example.com, if I searched for people with the email address jill@example.com,
    # john@example.com would show up because elasticsearch matched example w/ example.
    # By including "index": "not_analyzed", Elasticsearch leaves all owner inputs alone.
    index_name = Config.get_es_index_name(ESIndexType.subscriptions, Replica[replica])
    IndexManager.get_subscription_index(es_client, index_name, index_mapping)

    #  get all indexes that use current alias
    alias_name = Config.get_es_alias_name(ESIndexType.docs, Replica[replica])
    doc_indexes = _get_indexes_by_alias(es_client, alias_name)

    #  try to subscribe query to each of the indexes.
    subscribed_indexes = []
    for doc_index in doc_indexes:
        try:
            percolate_registration = _register_percolate(es_client, doc_index, uuid, es_query, replica)
        except ElasticsearchException as ex:
            logger.debug(f"Exception occured when registering a document to an index. Exception: {ex}")
            last_ex = ex
        else:
            logger.debug(f"Percolate query registration succeeded:\n{percolate_registration}")
            subscribed_indexes.append(doc_index)

    # Queries are unlikely to fit in all of the indexes, therefore errors will almost always occur. Only return an error
    # if no queries are successfully indexed.
    if doc_indexes and not subscribed_indexes:
        logger.critical(f"Percolate query registration failed: owner: {owner}, uuid: {uuid}, "
                        f"replica: {replica}, es_query: {es_query}, Exception: {last_ex}")
        raise DSSException(requests.codes.internal_server_error,
                           "elasticsearch_error",
                           "Unable to register elasticsearch percolate query!") from last_ex

    json_request_body['owner'] = owner

    try:
        subscription_registration = _register_subscription(es_client, uuid, json_request_body, replica)
        logger.debug(f"Event Subscription succeeded:\n{subscription_registration}")
    except ElasticsearchException as ex:
        logger.critical(f"Event Subscription failed: owner: {owner}, uuid: {uuid}, "
                        f"replica: {replica}, Exception: {ex}")

        # Delete percolate query to make sure queries and subscriptions are in sync.
        doc_indexes = _get_indexes_by_alias(es_client, alias_name)
        _unregister_percolate(es_client, doc_indexes, uuid)

        raise DSSException(requests.codes.internal_server_error,
                           "elasticsearch_error",
                           "Unable to register subscription! Rolling back percolate query.")

    return jsonify(dict(uuid=uuid)), requests.codes.created


@dss_handler
def delete(uuid: str, replica: str):
    authenticated_user_email = request.token_info['email']

    es_client = ElasticsearchClient.get()

    try:
        response = es_client.get(index=Config.get_es_index_name(ESIndexType.subscriptions, Replica[replica]),
                                 doc_type=ESDocType.subscription.name,
                                 id=uuid)
    except NotFoundError as ex:
        raise DSSException(requests.codes.not_found, "not_found", "Cannot find subscription!")

    stored_metadata = response['_source']

    if stored_metadata['owner'] != authenticated_user_email:
        # common_error_handler defaults code to capitalized 'Forbidden' for Werkzeug exception. Keeping consistent.
        raise DSSException(requests.codes.forbidden, "Forbidden", "Your credentials can't access this subscription!")

    #  get all indexes that use current alias
    alias_name = Config.get_es_alias_name(ESIndexType.docs, Replica[replica])
    doc_indexes = _get_indexes_by_alias(es_client, alias_name)
    _unregister_percolate(es_client, doc_indexes, uuid)

    es_client.delete(index=Config.get_es_index_name(ESIndexType.subscriptions, Replica[replica]),
                     doc_type=ESDocType.subscription.name,
                     id=uuid)

    timestamp = datetime.datetime.utcnow()
    time_deleted = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

    return jsonify({'timeDeleted': time_deleted}), requests.codes.okay


def _unregister_percolate(es_client: Elasticsearch, subscribed_indexes: List[str], uuid: str):
    response = es_client.delete_by_query(index=subscribed_indexes,
                                         doc_type=ESDocType.query.name,
                                         body={"query": {"ids": {"type": ESDocType.query.name, "values": [uuid]}}},
                                         conflicts="proceed",
                                         refresh=True)
    if response['failures']:
        logger.error("Failed to unregister percolate query for subscription %s: %s", uuid, response)


def _register_percolate(es_client: Elasticsearch, index_name: str, uuid: str, es_query: dict, replica: str):
    return es_client.index(index=index_name,
                           doc_type=ESDocType.query.name,
                           id=uuid,
                           body=es_query,
                           refresh=True)


def _register_subscription(es_client: Elasticsearch, uuid: str, json_request_body: dict, replica: str):
    index_name = Config.get_es_index_name(ESIndexType.subscriptions, Replica[replica])
    return es_client.index(index=index_name,
                           doc_type=ESDocType.subscription.name,
                           id=uuid,
                           body=json_request_body,
                           refresh=True)


def _get_indexes_by_alias(es_client: Elasticsearch, alias_name: str):
    try:
        return list(es_client.indices.get_alias(alias_name).keys())
    except NotFoundError:
        return []

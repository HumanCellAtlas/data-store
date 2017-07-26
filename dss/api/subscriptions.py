import datetime
import io
import json
import re
import typing
from uuid import uuid4

import iso8601
import requests

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Search
from flask import jsonify, make_response, redirect, request
from werkzeug.exceptions import BadRequest

from .. import (get_logger
                DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE, DSS_ELASTICSEARCH_QUERY_TYPE
                DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE)
from ..blobstore import BlobNotFoundError
from ..config import Config
from ..hcablobstore import FileMetadata, HCABlobStore
from ..util.es import ElasticsearchClient, get_elasticsearch_index

logger = get_logger()


def get(uuid: str, replica: str):
    owner = request.token_info['email']

    es_client = ElasticsearchClient.get(logger)
    id_exists = es_client.exists(index=DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME,
                                 doc_type=DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE,
                                 id=uuid)
    if not id_exists:
        return jsonify(dict(message=f"Subscription {uuid} does not exist",
                            exception="Placeholder",
                            HTTPStatusCode=requests.codes.not_found)), requests.codes.not_found

    response = es_client.get(index=DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME,
                             doc_type=DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE,
                             id=uuid)

    source = response['_source']
    source['uuid'] = uuid
    source['replica'] = replica

    if source['owner'] != owner:
        return jsonify(dict(
            message=f"You don't own subscription {uuid}.",
            exception="Placeholder",
            HTTPStatusCode=requests.codes.forbidden)), requests.codes.forbidden

    return jsonify(source), requests.codes.okay


def find(replica: str):
    owner = request.token_info['email']
    es_client = ElasticsearchClient.get(logger)

    search_obj = Search(using=es_client,
                        index=DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME,
                        doc_type=DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE)
    search = search_obj.query({'match': {'owner': owner}})

    responses = [{
        'uuid': hit.meta.id,
        'replica': replica,
        'owner': owner,
        'callback_url': hit.callback_url,
        'query': hit.query.to_dict()}
        for hit in search.scan()]

    full_response = {'subscriptions': responses}
    return jsonify(full_response), requests.codes.okay


def put(extras: dict, replica: str):
    uuid = str(uuid4())
    query = extras['query']
    owner = request.token_info['email']

    es_client = ElasticsearchClient.get(logger)

    index_mapping = {
        "mappings": {
            DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE: {
                "properties": {
                    "owner": {
                        "type": "string",
                        "index": "not_analyzed"
                    }
                }
            }
        }
    }
    get_elasticsearch_index(es_client, DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, logger, index_mapping)

    percolate_registration = _register_percolate(es_client, uuid, query)

    if percolate_registration['created']:
        logger.debug(f"Percolate query registration succeeded:\n{percolate_registration}")
    else:
        logger.critical(f"Percolate query registration failed:\n{percolate_registration}")
        return (jsonify(dict(
            message=f"Unable to register elasticsearch percolate query {uuid}.",
            exception="Placeholder",
            HTTPStatusCode=requests.codes.internal_server_error)),
            requests.codes.internal_server_error)

    extras['owner'] = owner
    subscription_registration = _register_subscription(es_client, uuid, extras)
    if subscription_registration['created']:
        logger.debug(f"Event Subscription succeeded:\n{subscription_registration}")
    else:
        logger.critical(f"Event Subscription failed:\n{subscription_registration}")

        # Delete percolate query to make sure queries and subscriptions are in sync.
        es_client.delete(index=DSS_ELASTICSEARCH_INDEX_NAME,
                         doc_type=DSS_ELASTICSEARCH_QUERY_TYPE,
                         id=uuid,
                         refresh=True)

        return (jsonify(dict(
            message=f"Unable to register elasticsearch percolate query {uuid}.",
            exception="Placeholder",
            HTTPStatusCode=requests.codes.internal_server_error)),
            requests.codes.internal_server_error)

    return jsonify(dict(uuid=uuid)), requests.codes.created


def delete(uuid: str, replica: str):
    owner = request.token_info['email']

    es_client = ElasticsearchClient.get(logger)
    id_exists = es_client.exists(index=DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME,
                                 doc_type=DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE,
                                 id=uuid)
    if not id_exists:
        return (jsonify(dict(
                message=f"Subscription {uuid} does not exist",
                exception="Placeholder",
                HTTPStatusCode=requests.codes.not_found)),
                requests.codes.not_found)

    response = es_client.get(index=DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME,
                             doc_type=DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE,
                             id=uuid)

    source = response['_source']

    if source['owner'] != owner:
        return (jsonify(dict(
            message=f"You don't have rights to delete subscription {uuid}.",
            exception="Placeholder",
            HTTPStatusCode=requests.codes.forbidden)),
            requests.codes.forbidden)

    es_client.delete(index=DSS_ELASTICSEARCH_INDEX_NAME,
                     doc_type=DSS_ELASTICSEARCH_QUERY_TYPE,
                     id=uuid,
                     refresh=True)

    es_client.delete(index=DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME,
                     doc_type=DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE,
                     id=uuid)

    timestamp = datetime.datetime.utcnow()
    time_deleted = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

    return jsonify({'timeDeleted': time_deleted}), requests.codes.okay


def _register_percolate(es_client: Elasticsearch, uuid: str, query: dict):
    return es_client.index(index=DSS_ELASTICSEARCH_INDEX_NAME,
                           doc_type=DSS_ELASTICSEARCH_QUERY_TYPE,
                           id=uuid,
                           body=query,
                           refresh=True)


def _register_subscription(es_client: Elasticsearch, uuid: str, extras: dict):
    return es_client.index(index=DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME,
                           doc_type=DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE,
                           id=uuid,
                           body=extras,
                           refresh=True)

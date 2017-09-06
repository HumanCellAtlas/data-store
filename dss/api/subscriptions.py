import datetime
import io
import json
import re
import typing
from uuid import uuid4

import iso8601
import requests

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ElasticsearchException, NotFoundError
from elasticsearch_dsl import Search
from flask import jsonify, make_response, redirect, request
from werkzeug.exceptions import BadRequest

from .. import (
    get_logger,
    DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE, DSS_ELASTICSEARCH_QUERY_TYPE,
    DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE)
from ..blobstore import BlobNotFoundError
from ..config import Config
from ..error import DSSException, dss_handler
from ..hcablobstore import FileMetadata, HCABlobStore
from ..util.es import ElasticsearchClient, get_elasticsearch_index_name, get_elasticsearch_index

logger = get_logger()


@dss_handler
def get(uuid: str, replica: str):
    owner = request.token_info['email']

    es_client = ElasticsearchClient.get(logger)

    try:
        response = es_client.get(index=get_elasticsearch_index_name(DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, replica),
                                 doc_type=DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE,
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
    es_client = ElasticsearchClient.get(logger)

    search_obj = Search(using=es_client,
                        index=get_elasticsearch_index_name(DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, replica),
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


@dss_handler
def put(json_request_body: dict, replica: str):
    uuid = str(uuid4())
    query = json_request_body['query']
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
    # Elasticsearch preprocesses inputs by splitting strings on punctuation.
    # So for john@example.com, if I searched for people with the email address jill@example.com,
    # john@example.com would show up because elasticsearch matched example w/ example.
    # By including "index": "not_analyzed", Elasticsearch leaves all owner inputs alone.
    index_name = get_elasticsearch_index_name(DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, replica)
    get_elasticsearch_index(es_client, index_name, logger, index_mapping)

    try:
        percolate_registration = _register_percolate(es_client, uuid, query, replica)
        logger.debug(f"Percolate query registration succeeded:\n{percolate_registration}")
    except ElasticsearchException:
        logger.critical(f"Percolate query registration failed:\n{percolate_registration}")
        raise DSSException(requests.codes.internal_server_error,
                           "elasticsearch_error",
                           "Unable to register elasticsearch percolate query!")

    json_request_body['owner'] = owner

    try:
        subscription_registration = _register_subscription(es_client, uuid, json_request_body, replica)
        logger.debug(f"Event Subscription succeeded:\n{subscription_registration}")
    except ElasticsearchException:
        logger.critical(f"Event Subscription failed:\n{subscription_registration}")

        # Delete percolate query to make sure queries and subscriptions are in sync.
        es_client.delete(index=index_name,
                         doc_type=DSS_ELASTICSEARCH_QUERY_TYPE,
                         id=uuid,
                         refresh=True)
        raise DSSException(requests.codes.internal_server_error,
                           "elasticsearch_error",
                           "Unable to register subscription! Rolling back percolate query.")

    return jsonify(dict(uuid=uuid)), requests.codes.created


@dss_handler
def delete(uuid: str, replica: str):
    authenticated_user_email = request.token_info['email']

    es_client = ElasticsearchClient.get(logger)

    try:
        response = es_client.get(index=get_elasticsearch_index_name(DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, replica),
                                 doc_type=DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE,
                                 id=uuid)
    except NotFoundError as ex:
        raise DSSException(requests.codes.not_found, "not_found", "Cannot find subscription!")

    stored_metadata = response['_source']

    if stored_metadata['owner'] != authenticated_user_email:
        # common_error_handler defaults code to capitalized 'Forbidden' for Werkzeug exception. Keeping consistent.
        raise DSSException(requests.codes.forbidden, "Forbidden", "Your credentials can't access this subscription!")

    es_client.delete(index=get_elasticsearch_index_name(DSS_ELASTICSEARCH_INDEX_NAME, replica),
                     doc_type=DSS_ELASTICSEARCH_QUERY_TYPE,
                     id=uuid,
                     refresh=True)

    es_client.delete(index=get_elasticsearch_index_name(DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, replica),
                     doc_type=DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE,
                     id=uuid)

    timestamp = datetime.datetime.utcnow()
    time_deleted = timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

    return jsonify({'timeDeleted': time_deleted}), requests.codes.okay


def _register_percolate(es_client: Elasticsearch, uuid: str, query: dict, replica: str):
    index_name = get_elasticsearch_index_name(DSS_ELASTICSEARCH_INDEX_NAME, replica)
    return es_client.index(index=index_name,
                           doc_type=DSS_ELASTICSEARCH_QUERY_TYPE,
                           id=uuid,
                           body=query,
                           refresh=True)


def _register_subscription(es_client: Elasticsearch, uuid: str, json_request_body: dict, replica: str):
    index_name = get_elasticsearch_index_name(DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, replica)
    return es_client.index(index=index_name,
                           doc_type=DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE,
                           id=uuid,
                           body=json_request_body,
                           refresh=True)

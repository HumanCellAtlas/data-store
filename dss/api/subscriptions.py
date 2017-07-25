import datetime
import io
import json
import re
import typing
import uuid as uuid_

import iso8601
import requests

from flask import jsonify, make_response, redirect, request
from werkzeug.exceptions import BadRequest

from .. import get_logger
from .. import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE, DSS_ELASTICSEARCH_QUERY_TYPE
from .. import DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE
from ..blobstore import BlobNotFoundError
from ..config import Config
from ..hcablobstore import FileMetadata, HCABlobStore
from ..util.es import ElasticsearchClient, get_elasticsearch_index

logger = get_logger()


def get(uuid: str, replica: str):
    pass


def find(replica: str):
    pass


def put(extras: dict, replica: str):
    uuid = str(uuid_.uuid4())
    query = extras['query']

    es_client = ElasticsearchClient.get(logger)
    get_elasticsearch_index(es_client, DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, logger)

    percolate_registration = _register_percolate(es_client, uuid, query)

    if percolate_registration['created']:
        logger.debug("Percolate query registration succeeded:\n{}".format(percolate_registration))
    else:
        logger.critical("Percolate query registration failed:\n{}".format(percolate_registration))
        return (jsonify(dict(
            message="Unable to register elasticsearch percolate query {}.".format(uuid),
            exception=str(ex),
            HTTPStatusCode=requests.codes.internal_server_error)),
            requests.codes.internal_server_error)

    subscription_registration = _register_subscription(es_client, uuid, extras)
    if subscription_registration['created']:
        logger.debug("Event Subscription succeeded:\n{}".format(subscription_registration))
    else:
        logger.critical("Event Subscription failed:\n{}".format(subscription_registration))
        return (jsonify(dict(
            message="Unable to register elasticsearch percolate query {}.".format(uuid),
            exception=str(ex),
            HTTPStatusCode=requests.codes.internal_server_error)),
            requests.codes.internal_server_error)

    return jsonify(dict(uuid=uuid)), requests.codes.created


def delete(uuid: str, replica: str):
    pass


def _register_percolate(es_client: Elasticsearch, uuid: str, query: dict):
    return es_client.index(index=DSS_ELASTICSEARCH_INDEX_NAME,
                           doc_type=DSS_ELASTICSEARCH_QUERIES_DOC_TYPE,
                           id=uuid,
                           body=query,
                           refresh=True)


def _register_subscription(es_client: Elasticsearch, uuid: str, extras: dict):
    return es_client.index(index=DSS_ELASTICSEARCH_QUERIES_INDEX_NAME,
                           doc_type=DSS_ELASTICSEARCH_QUERIES_DOC_TYPE,
                           id=uuid,
                           body=extras,
                           refresh=True)
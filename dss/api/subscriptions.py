import datetime
import io
import json
import os
import re
import typing

import iso8601
import requests

from elasticsearch import Elasticsearch
from flask import jsonify, make_response, redirect, request
from werkzeug.exceptions import BadRequest

from .. import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE, get_logger
from .. import DSS_ELASTICSEARCH_QUERIES_INDEX_NAME, DSS_ELASTICSEARCH_QUERIES_DOC_TYPE
from ..blobstore import BlobNotFoundError
from ..config import Config
from ..hcablobstore import FileMetadata, HCABlobStore
from ..util import connect_elasticsearch


logger = get_logger()
ELASTICSEARCH_ENDPOINT = os.getenv('DSS_ES_ENDPOINT')
ELASTICSEARCH_INDICES = [DSS_ELASTICSEARCH_INDEX_NAME,
                         DSS_ELASTICSEARCH_QUERIES_INDEX_NAME]


def get(uuid: str, replica: str):
    pass


def put(uuid: str, extras: dict, replica: str):
    pass


def delete(uuid: str, replica: str):
    pass


def _connect_elasticsearch_with_indices_defined() -> Elasticsearch:
    es_client = connect_elasticsearch(ELASTICSEARCH_ENDPOINT, logger)
    for idx in ELASTICSEARCH_INDICES:
        _create_elasticsearch_index(es_client, idx)
    return es_client


def _create_elasticsearch_index(es_client, idx):
    try:
        response = es_client.indices.exists(idx)
        if response:
            logger.debug("Using existing Elasticsearch index: {}".format(idx))
        else:
            logger.debug("Creating new Elasticsearch index: {}".format(idx))
            index_mapping = None
            if idx == DSS_ELASTICSEARCH_INDEX_NAME:
                index_mapping = {
                    "mappings": {
                        DSS_ELASTICSEARCH_QUERIES_DOC_TYPE: {
                            "properties": {
                                "query": {
                                    "type": "percolator"
                                }
                            }
                        }
                    }
                }
            response = es_client.indices.create(idx, body=index_mapping)
            logger.debug("Index creation response: {}", (json.dumps(response, indent=4)))

    except Exception as ex:
        logger.critical("Unable to create index {} Exception: {}".format(idx))
        return (jsonify(dict(
            message="Unable to create elasticsearch index {}.".format(idx),
            exception=str(ex),
            HTTPStatusCode=requests.codes.internal_server_error)),
            requests.codes.internal_server_error)

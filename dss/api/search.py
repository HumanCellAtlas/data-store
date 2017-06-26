import json
import os

from elasticsearch_dsl import Search
from flask import request

from .. import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE
from .. import get_logger
from ..util import connect_elasticsearch


def list():
    es_client = connect_elasticsearch(os.getenv("DSS_ES_ENDPOINT"), get_logger())  # TODO Use a connection manager
    get_logger().debug("Searching for: %s", request.values["query"])
    query = json.loads(request.values["query"])
    response = Search(using=es_client,
                      index=DSS_ELASTICSEARCH_INDEX_NAME,
                      doc_type=DSS_ELASTICSEARCH_DOC_TYPE).query("match", **query).execute()
    return {"query": query, "results": format_results(request, response)}


def post(query: dict):
    get_logger().debug("Received posted query: %s", json.dumps(query, indent=4))
    es_client = connect_elasticsearch(os.getenv("DSS_ES_ENDPOINT"), get_logger())  # TODO Use a connection manager
    response = Search(using=es_client,
                      index=DSS_ELASTICSEARCH_INDEX_NAME,
                      doc_type=DSS_ELASTICSEARCH_DOC_TYPE).update_from_dict(query).execute()
    return {"query": query, "results": format_results(request, response)}


def format_results(request, response):
    bundles_url_base = request.host_url + request.full_path[1:].replace('search?', 'bundles/')
    result_list = []
    for hit in response:
        result = {
            'bundle_id': hit.meta.id,
            'bundle_url': bundles_url_base + hit.meta.id,
            'search_score': hit.meta.score
        }
        result_list.append(result)
    return result_list

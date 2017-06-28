import json
import os

from elasticsearch_dsl import Search
from flask import request

from .. import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE
from .. import get_logger
from ..util import connect_elasticsearch


def list():
    get_logger().debug("Searching for: %s", request.values["query"])
    # TODO (mbaumann) Use a connection manager
    es_client = connect_elasticsearch(os.getenv("DSS_ES_ENDPOINT"), get_logger())
    query = json.loads(request.values["query"])
    response = Search(using=es_client,
                      index=DSS_ELASTICSEARCH_INDEX_NAME,
                      doc_type=DSS_ELASTICSEARCH_DOC_TYPE).query("match", **query).execute()
    return {"query": query, "results": format_results(request, response)}


def post(extras: dict):
    query = extras
    get_logger().debug("Received posted query: %s", json.dumps(query, indent=4))
    # TODO (mbaumann) Use a connection manager
    es_client = connect_elasticsearch(os.getenv("DSS_ES_ENDPOINT"), get_logger())
    response = Search(using=es_client,
                      index=DSS_ELASTICSEARCH_INDEX_NAME,
                      doc_type=DSS_ELASTICSEARCH_DOC_TYPE).update_from_dict(query).execute()
    return {"query": query, "results": format_results(request, response)}


def format_results(request, response):
    # TODO (mbaumann) extract version from the request path instead of hard-coding it here
    # The previous code worked for post but incorrectly included the query string in the case of get.
    bundles_url_base = request.host_url + 'v1/bundles/'
    result_list = []
    for hit in response:
        result = {
            'bundle_id': hit.meta.id,
            'bundle_url': bundles_url_base + hit.meta.id,
            'search_score': hit.meta.score
        }
        result_list.append(result)
    return result_list

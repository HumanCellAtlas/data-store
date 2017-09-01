import json
import os

from elasticsearch_dsl import Search
from flask import request

from .. import Config, Replica, ESIndexType, dss_handler, get_logger
from ..util.es import ElasticsearchClient

# TODO Adding replica as a search parameter and including tests for gcp
# will be done in a different PR.
replica = "aws"

@dss_handler
def find():
    get_logger().debug("Searching for: %s", request.values["query"])
    # TODO (mbaumann) Use a connection manager
    es_client = ElasticsearchClient.get(get_logger())
    query = json.loads(request.values["query"])
    response = Search(using=es_client,
                      index=Config.get_es_index_name(ESIndexType.docs, Replica[replica]),
                      doc_type=ESIndexType.docs).query("match", **query).execute()
    return {"query": query, "results": format_results(request, response)}


@dss_handler
def post(query: dict):
    get_logger().debug("Received posted query: %s", json.dumps(query, indent=4))
    # TODO (mbaumann) Use a connection manager
    es_client = ElasticsearchClient.get(get_logger())
    response = Search(using=es_client,
                      index=Config.get_es_index_name(ESIndexType.docs, Replica[replica]),
                      doc_type=ESIndexType.docs).update_from_dict(query).execute()
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

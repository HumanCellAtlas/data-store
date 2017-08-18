import json

import requests
from elasticsearch.exceptions import ElasticsearchException
from elasticsearch_dsl import Search
from elasticsearch_dsl.exceptions import ElasticsearchDslException
from flask import request, jsonify

from .. import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE
from .. import dss_handler, get_logger, DSSException
from ..util.es import ElasticsearchClient


@dss_handler
def post(query: dict):
    get_logger().debug("Received posted query: %s", json.dumps(query, indent=4))
    try:
        es_client = ElasticsearchClient.get(get_logger())
        search_obj = Search(using=es_client,
                            index=DSS_ELASTICSEARCH_INDEX_NAME,
                            doc_type=DSS_ELASTICSEARCH_DOC_TYPE).update_from_dict(query)
        return jsonify({"query": query, "results": format_results(request, search_obj)})

    except ElasticsearchDslException:
        raise DSSException(requests.codes.bad_request,
                           "elasticsearch_query_error",
                           "Invalid query")
    except ElasticsearchException:
        raise DSSException(requests.codes.internal_server_error,
                           "elasticsearch_error",
                           "Elasticsearch operation failed")

def format_results(request, search_obj):
    # TODO (mbaumann) extract version from the request path instead of hard-coding it here
    # The previous code worked for post but incorrectly included the query string in the case of get.
    bundles_url_base = request.host_url + 'v1/bundles/'
    result_list = [{
        'bundle_id': hit.meta.id,
        'bundle_url': bundles_url_base + hit.meta.id.replace(".", "?version=", 1),
        'search_score': hit.meta.score
    } for hit in search_obj.scan()]
    return result_list

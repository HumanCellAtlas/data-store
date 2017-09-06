import json

import requests
from elasticsearch.exceptions import ElasticsearchException
from elasticsearch_dsl import Search
from elasticsearch_dsl.exceptions import ElasticsearchDslException
from flask import request, jsonify

from .. import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE
from .. import dss_handler, get_logger, DSSException
from ..util.es import ElasticsearchClient, get_elasticsearch_index_name

# TODO Adding replica as a search parameter and including tests for gcp
# will be done in a different PR.
replica = "aws"

@dss_handler
def post(json_request_body: dict):
    es_query = json_request_body['es_query']
    get_logger().debug("Received posted query: %s", json.dumps(es_query, indent=4))
    try:
        es_client = ElasticsearchClient.get(get_logger())
        search_obj = Search(using=es_client,
                            index=get_elasticsearch_index_name(DSS_ELASTICSEARCH_INDEX_NAME, replica),
                            doc_type=DSS_ELASTICSEARCH_DOC_TYPE).update_from_dict(es_query)

        # TODO (mbaumann) extract version from the request path instead of hard-coding it here
        bundles_url_base = request.host_url + 'v1/bundles/'
        result_list = [{
            'bundle_id': hit.meta.id,
            'bundle_url': bundles_url_base + hit.meta.id.replace(".", "?version=", 1),
            'search_score': hit.meta.score
        } for hit in search_obj.scan()]
        return jsonify({'es_query': es_query, 'results': result_list})

    except ElasticsearchDslException:
        raise DSSException(requests.codes.bad_request,
                           "elasticsearch_query_error",
                           "Invalid query")
    except ElasticsearchException:
        raise DSSException(requests.codes.internal_server_error,
                           "elasticsearch_error",
                           "Elasticsearch operation failed")

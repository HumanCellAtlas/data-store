import json

import requests
from elasticsearch.exceptions import ElasticsearchException
from elasticsearch_dsl import Search
from elasticsearch_dsl.exceptions import ElasticsearchDslException
from flask import request, jsonify

from dss import ESDocType
from .. import Config, Replica, ESIndexType, dss_handler, get_logger, DSSException
from ..util.es import ElasticsearchClient

@dss_handler
def post(json_request_body: dict, replica: str):
    es_query = json_request_body['es_query']
    get_logger().debug("Received posted query. Replica: %s Query: %s", replica, json.dumps(es_query, indent=4))
    if replica is None:
        replica = "aws"
    try:
        es_client = ElasticsearchClient.get(get_logger())
        search_obj = Search(using=es_client,
                            index=Config.get_es_index_name(ESIndexType.docs, Replica[replica]),
                            doc_type=ESDocType.doc.name).update_from_dict(es_query)

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

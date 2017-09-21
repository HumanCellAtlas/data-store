import json
import typing

import requests
from elasticsearch.exceptions import ElasticsearchException, TransportError
from flask import request, jsonify

from dss import ESDocType
from .. import Config, Replica, ESIndexType, dss_handler, get_logger, DSSException
from ..util import UrlBuilder
from ..util.es import ElasticsearchClient


class PerPageBounds:
    per_page_max = 500
    per_page_min = 10

    @classmethod
    def check(cls, n):
        return max(min(cls.per_page_max, n), cls.per_page_min)

@dss_handler
def post(json_request_body: dict, replica: str, per_page: int, _scroll_id: typing.Optional[str] = None) -> dict:
    es_query = json_request_body['es_query']
    get_logger().debug("Received posted query. Replica: %s Query: %s Per_page: %i Timeout: %s Scroll_id: %s",
                       replica, json.dumps(es_query, indent=4), per_page, _scroll_id)
    if replica is None:
        replica = "aws"
    per_page = PerPageBounds.check(per_page)

    # The time for a scroll search context to stay open per page. A page of results must be retreived before this
    # timeout expires. Subsequent calls to search will refresh the scroll timeout. For more details on time format see:
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#time-units
    scroll = '2m'  # set a timeout of 2min to keep the search context alive. This is reset

    # From: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
    # Scroll requests have optimizations that make them faster when the sort order is _doc. If you want to iterate over
    # all documents regardless of the order, this is the most efficient option:
    # {
    #   "sort": [
    #     "_doc"
    #   ]
    # }
    sort = {"sort": ["_doc"]}

    # TODO: (tsmith12) determine if a search operation timeout limit is needed
    try:
        es_client = ElasticsearchClient.get(get_logger())
        if _scroll_id is None:
            page = es_client.search(index=Config.get_es_index_name(ESIndexType.docs, Replica[replica]),
                                    doc_type=ESDocType.doc.name,
                                    scroll=scroll,
                                    size=per_page,
                                    body=es_query,
                                    sort=sort
                                    )
            get_logger().debug("Created ES scroll instance")
        else:
            get_logger().debug("Retrieve ES results from scroll instance Scroll_id: %s", _scroll_id)
            page = es_client.scroll(scroll_id=_scroll_id, scroll=scroll)
            # if page returns 0 hits, then all results have been found. Delete search_id
            if len(page['hits']['hits']) == 0:
                es_client.clear_scroll(_scroll_id)
                get_logger().debug("Deleted ES scroll instance Scroll_id: %s", _scroll_id)
        _scroll_id = page['_scroll_id']
        result_list = [{
            'bundle_id': hit['_id'],
            'bundle_url': _build_bundle_url(hit, replica),
            'search_score': hit['_score']
        } for hit in page['hits']['hits']]
        next_url = request.host_url + str(UrlBuilder().set(path="v1/search")
                                          .add_query("replica", replica)
                                          .add_query("_scroll_id", _scroll_id))
        return jsonify({'es_query': es_query, 'results': result_list, 'next_url': next_url})

    except TransportError as ex:
        if ex.status_code == requests.codes.bad_request:
            get_logger().debug("%s", f"Invalid Query Recieved. Exception: {ex}")
            raise DSSException(requests.codes.bad_request,
                               "elasticsearch_bad_request",
                               f"Invalid Elasticsearch query was received: {str(ex)}")
        elif ex.status_code == requests.codes.not_found:
            get_logger().debug("%s", f"Search Context Error. Exception: {ex}")
            raise DSSException(requests.codes.not_found,
                               "elasticsearch_context_not_found",
                               "Elasticsearch context has returned all results or timeout has expired.")
        else:
            get_logger().error("%s", f"Elasticsearch Internal Server Error. Exception: {ex}")
            raise DSSException(requests.codes.internal_server_error,
                               "internal_server_error",
                               "Elasticsearch Internal Server Error")

    except ElasticsearchException as ex:
        get_logger().error("%s", f"Elasticsearch Internal Server Error. Exception: {ex}")
        raise DSSException(requests.codes.internal_server_error,
                           "internal_server_error",
                           "Elasticsearch Internal Server Error")


def _build_bundle_url(hit: dict, replica: str) -> str:
    uuid, version = hit['_id'].split('.', 1)
    return request.host_url + str(UrlBuilder().set(path='v1/bundles/' + uuid)
                                  .add_query("version", version)
                                  .add_query("replica", replica))

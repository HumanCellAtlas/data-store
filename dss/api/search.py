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
def post(json_request_body: dict, replica: str, per_page: typing.Optional[int] = 100,
         scroll: typing.Optional[str] = '1m', _scroll_id: typing.Optional[str] = None) -> dict:
    es_query = json_request_body['es_query']
    get_logger().debug("Received posted query. Replica: %s Query: %s Per_page: %i Timeout: %s Scroll_id: %s",
                       replica, json.dumps(es_query, indent=4), per_page, scroll, _scroll_id)
    if replica is None:
        replica = "aws"
    per_page = PerPageBounds.check(per_page)
    # if sort is not defined used the following
    sort = {
        "sort": [
            "_doc"
        ]
    }

    try:
        es_client = ElasticsearchClient.get(get_logger())
        # TODO parse es_query to prevent delete query or other malicious attacks

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
            # TODO: if page returns 0 hits, then all results have been found. Delete search_id
            if not len(page['hits']['hits']):
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
                                          .add_query("scroll", scroll)
                                          .add_query("_scroll_id", _scroll_id))
        return jsonify({'es_query': es_query, 'results': result_list, 'next_url': next_url})

    except TransportError as ex:
        if ex.status_code == requests.codes.bad_request:
            get_logger().debug("%s", f"Invalid Query Recieved. Exception: {ex}")
            raise DSSException(requests.codes.bad_request,
                               f"Elasticsearch Invalid Query",
                               str(ex))
        elif ex.status_code == requests.codes.not_found:
            get_logger().debug("%s", f"Elasticsearch search context has expired. Exception: {ex}")
            raise DSSException(requests.codes.not_found,
                               f"Elasticsearch Page Expired",
                               "Search context has expired.")
        else:
            get_logger().error("%s", f"Elasticsearch Internal Server Error. Exception: {ex}")
            raise DSSException(requests.codes.internal_server_error,
                               f"Elasticsearch Internal Server Error",
                               "Internal Server Error")

    except ElasticsearchException as ex:
        get_logger().error("%s", f"Elasticsearch Internal Server Error. Exception: {ex}")
        raise DSSException(requests.codes.internal_server_error,
                           f"Elasticsearch Internal Server Error",
                           "Internal Server Error")


def _build_bundle_url(hit: dict, replica: str) -> str:
    uuid, version = hit['_id'].split('.', 1)
    return request.host_url + str(UrlBuilder().set(path='v1/bundles/' + uuid)
                                  .add_query("version", version)
                                  .add_query("replica", replica))

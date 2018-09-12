import json
import logging
import typing

import requests
from copy import deepcopy
from elasticsearch.exceptions import ElasticsearchException, TransportError
from flask import request, jsonify, make_response

from dss import ESDocType
from dss import Config, Replica, ESIndexType, dss_handler, DSSException
from dss.util import UrlBuilder
from dss.index.es import ElasticsearchClient


logger = logging.getLogger(__name__)


class PerPageBounds:
    per_page_max = 500
    per_page_min = 10

    @classmethod
    def check(cls, n):
        """Limits per_page from exceed its min and max value"""
        return max(min(cls.per_page_max, n), cls.per_page_min)


@dss_handler
def post(json_request_body: dict,
         replica: str,
         per_page: int,
         output_format: str,
         search_after: typing.Optional[str] = None) -> dict:
    es_query = json_request_body['es_query']
    per_page = PerPageBounds.check(per_page)

    replica_enum = Replica[replica] if replica is not None else Replica.aws

    logger.debug("Received POST for replica=%s, es_query=%s, per_page=%i, search_after: %s",
                 replica_enum.name, json.dumps(es_query, indent=4), per_page, search_after)

    # TODO: (tsmith12) allow users to retrieve previous search results
    try:
        page = _es_search_page(es_query, replica_enum, per_page, search_after, output_format)
        request_dict = _format_request_body(page, es_query, replica_enum, output_format)
        request_body = jsonify(request_dict)

        if len(request_dict['results']) < per_page:
            response = make_response(request_body, requests.codes.ok)
        else:
            response = make_response(request_body, requests.codes.partial)
            next_url = _build_next_url(page, per_page, replica_enum, output_format)
            response.headers['Link'] = _build_link_header({next_url: {"rel": "next"}})
        return response
    except TransportError as ex:
        if ex.status_code == requests.codes.bad_request:
            logger.debug(f"Invalid Query Recieved. Exception: {ex}")
            raise DSSException(requests.codes.bad_request,
                               "elasticsearch_bad_request",
                               f"Invalid Elasticsearch query was received: {str(ex)}")
        elif ex.status_code == requests.codes.not_found:
            logger.debug(f"Search Context Error. Exception: {ex}")
            raise DSSException(requests.codes.not_found,
                               "elasticsearch_context_not_found",
                               "Elasticsearch context has returned all results or timeout has expired.")
        elif ex.status_code == 'N/A':
            logger.error(f"Elasticsearch Invalid Endpoint. Exception: {ex}")
            raise DSSException(requests.codes.service_unavailable,
                               "service_unavailable",
                               "Elasticsearch reached an invalid endpoint. Try again later.")
        else:
            logger.error(f"Elasticsearch Internal Server Error. Exception: {ex}")
            raise DSSException(requests.codes.internal_server_error,
                               "internal_server_error",
                               "Elasticsearch Internal Server Error")

    except ElasticsearchException as ex:
        logger.error(f"Elasticsearch Internal Server Error. Exception: {ex}")
        raise DSSException(requests.codes.internal_server_error,
                           "internal_server_error",
                           "Elasticsearch Internal Server Error")


def _es_search_page(es_query: dict,
                    replica: Replica,
                    per_page: int,
                    search_after: typing.Optional[str],
                    output_format: str) -> dict:
    es_query = deepcopy(es_query)
    es_client = ElasticsearchClient.get()

    # Do not return the raw indexed data unless it is requested
    if output_format != 'raw':
        es_query['_source'] = False

    # https://www.elastic.co/guide/en/elasticsearch/reference/5.5/search-request-search-after.html
    sort = [
        "manifest.version:desc",
        "uuid:desc"]

    if search_after is None:
        page = es_client.search(index=Config.get_es_alias_name(ESIndexType.docs, replica),
                                doc_type=ESDocType.doc.name,
                                size=per_page,
                                body=es_query,
                                sort=sort
                                )
    else:
        es_query['search_after'] = search_after.split(',')
        page = es_client.search(index=Config.get_es_alias_name(ESIndexType.docs, replica),
                                doc_type=ESDocType.doc.name,
                                size=per_page,
                                body=es_query,
                                sort=sort,
                                )
        logger.debug(f"Retrieved ES results from page after: {search_after}")
    return page


def _format_request_body(page: dict, es_query: dict, replica: Replica, output_format: str) -> dict:
    result_list = []  # type: typing.List[dict]
    for hit in page['hits']['hits']:
        result = {
            'bundle_fqid': hit['_id'],
            'bundle_url': _build_bundle_url(hit, replica),
            'search_score': hit['_score']
        }
        if output_format == 'raw':
            result['metadata'] = hit['_source']
        result_list.append(result)

    return {
        'es_query': es_query,
        'results': result_list,
        'total_hits': page['hits']['total']
    }


def _build_bundle_url(hit: dict, replica: Replica) -> str:
    uuid, version = hit['_id'].split('.', 1)
    return request.host_url + str(UrlBuilder()
                                  .set(path='v1/bundles/' + uuid)
                                  .add_query("version", version)
                                  .add_query("replica", replica.name)
                                  )


def _build_next_url(page: dict, per_page: int, replica: Replica, output_format: str) -> str:
    search_after = ','.join(page['hits']['hits'][-1]['sort'])
    return request.host_url + str(UrlBuilder()
                                  .set(path="v1/search")
                                  .add_query('per_page', str(per_page))
                                  .add_query("replica", replica.name)
                                  .add_query("search_after", search_after)
                                  .add_query("output_format", output_format)
                                  )


def _build_link_header(links):
    """
    Builds a Link header according to RFC 5988.
    The format is a dict where the keys are the URI with the value being
    a dict of link parameters:
        {
            '/page=3': {
                'rel': 'next',
            },
            '/page=1': {
                'rel': 'prev',
            },
            ...
        }
    See https://tools.ietf.org/html/rfc5988#section-6.2.2 for registered
    link relation types.
    """
    _links = []
    for uri, params in links.items():
        link = [f"<{uri}>"]
        for key, value in params.items():
            link.append(f'{key}="{str(value)}"')
        _links.append('; '.join(link))
    return ', '.join(_links)

import os
import logging
from datetime import datetime

import requests
from flask import request, make_response, jsonify
from flashflood import FlashFlood, FlashFloodEventNotFound

from dss.util.aws import resources
from dss import Config, Replica
from dss import events
from dss.error import DSSException, dss_handler
from dss.util import security, hashabledict, UrlBuilder


logger = logging.getLogger(__name__)


@dss_handler
def list_events(replica: str, from_date: str=None, to_date: str=None, per_page: int=1, start_at: int = 0):
    ff = FlashFlood(resources.s3, Config.get_flashflood_bucket(), Replica[replica].flashflood_prefix)  # type: ignore
    urls = ff.replay_urls(events.datetime_from_timestamp(from_date) if from_date else datetime.min,
                          events.datetime_from_timestamp(to_date) if to_date else datetime.max,
                          per_page)
    if len(urls) < per_page:
        response = make_response(jsonify(urls), requests.codes.ok)
        response.headers['X-OpenAPI-Pagination'] = 'false'
    else:
        next_url = UrlBuilder(request.url)
        next_url.replace_query("from_date", urls[-1]['manifest']['to_date'])
        link = f"<{next_url}>; rel='next'"
        response = make_response(jsonify(urls), requests.codes.partial)
        response.headers['Link'] = link
        response.headers['X-OpenAPI-Pagination'] = 'true'
        response.headers['X-OpenAPI-Paginated-Content-Key'] = 'event_streams'
    return response

@dss_handler
def get(uuid: str, replica: str, version: str = None):
    key = f"bundles/{uuid}.{version}"
    doc = events.get_bundle_metadata_document(Replica[replica], key)
    return doc

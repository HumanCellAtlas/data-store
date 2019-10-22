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
from dss.util.version import datetime_from_timestamp


logger = logging.getLogger(__name__)


@dss_handler
def list_events(replica: str, from_date: str=None, to_date: str=None, per_page: int=1, token: str=None):
    if token:
        fdate = datetime_from_timestamp(token)
    else:
        fdate = datetime_from_timestamp(from_date) if from_date else datetime.min
    tdate = datetime_from_timestamp(to_date) if to_date else datetime.max
    if fdate > tdate:
        raise DSSException(400, "bad_request", "to_date must be greater than from_date")
    ff = FlashFlood(resources.s3, Config.get_flashflood_bucket(), Replica[replica].flashflood_prefix_read)
    event_streams = list()
    for i, event_stream in enumerate(ff.list_event_streams(fdate, tdate)):
        if datetime_from_timestamp(event_stream['from_date']) < tdate:
            event_streams.append(event_stream)
        else:
            break
        if i == per_page:
            break

    if len(event_streams) <= per_page:
        response = make_response(jsonify(dict(event_streams=event_streams)), requests.codes.ok)
        response.headers['X-OpenAPI-Pagination'] = 'false'
    else:
        next_url = UrlBuilder(request.url)
        next_url.replace_query("token", event_streams[-1]['from_date'])
        link = f"<{next_url}>; rel='next'"
        response = make_response(jsonify(dict(event_streams=event_streams[:-1])), requests.codes.partial)
        response.headers['Link'] = link
        response.headers['X-OpenAPI-Pagination'] = 'true'
    response.headers['X-OpenAPI-Paginated-Content-Key'] = 'event_streams'
    return response

@dss_handler
def get(uuid: str, replica: str, version: str = None):
    key = f"bundles/{uuid}.{version}"
    doc = events.get_bundle_metadata_document(Replica[replica], key)
    if doc is None:
        raise DSSException(404, "not_found", "Cannot find event!")
    return doc

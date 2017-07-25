#!/usr/bin/env python

"""
DSS description FIXME: elaborate
"""
import traceback

import os
import logging

import connexion
import flask
import requests
from connexion.resolver import RestyResolver
from flask_failsafe import failsafe

from .config import BucketStage, Config
from .error import DSSException, dss_handler

# CONSTANTS COMMON TO THE INDEXER AND QUERY ROUTE.

# ES index containing all docs
DSS_ELASTICSEARCH_INDEX_NAME = "hca"
# ES type within DSS_ELASTICSEARCH_INDEX_NAME with docs
DSS_ELASTICSEARCH_DOC_TYPE = "doc"
# ES type within DSS_ELASTICSEARCH_INDEX_NAME with percolate queries
DSS_ELASTICSEARCH_QUERY_TYPE = "query"

# ES index with all registered percolate queries
DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME = "subscriptions"
# ES type in DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME with subscriptions
DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE = "subscription"


def get_logger():
    try:
        return flask.current_app.logger
    except RuntimeError:
        return logging.getLogger(__name__)


class DSSApp(connexion.App):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def common_error_handler(exception):
        """
        Generally, each route handler should be decorated with @dss_handler, which manages exceptions.  The two cases
        that fails are:

        1. handlers that are not decorated.
        2. handlers that return a code that is not in the swagger spec.

        In both cases, the exception would be punted here, and we return this very generic error that also happens to
        bypass all validation.
        """
        return (
            flask.jsonify({
                'http-error-code': requests.codes.server_error,
                'code': "unhandled_exception",
                'message': str(exception),
                'stacktrace': traceback.format_exc(),
            }),
            requests.codes.server_error,
        )


@failsafe
def create_app():
    app = DSSApp(__name__)
    resolver = RestyResolver("dss.api", collection_endpoint_name="list")
    app.add_api('../dss-api.yml', resolver=resolver, validate_responses=True, arguments=os.environ)
    return app

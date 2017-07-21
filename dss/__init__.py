#!/usr/bin/env python

"""
DSS description FIXME: elaborate
"""

import logging

import flask
import connexion
from connexion.resolver import RestyResolver
from flask_failsafe import failsafe

from .config import BucketStage, Config

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

@failsafe
def create_app():
    app = connexion.App(__name__)
    resolver = RestyResolver("dss.api", collection_endpoint_name="list")
    app.add_api('../dss-api.yml', resolver=resolver, validate_responses=True)
    return app

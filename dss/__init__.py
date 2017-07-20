#!/usr/bin/env python

"""
DSS description FIXME: elaborate
"""

import os
import json
import logging

import flask
import connexion
import connexion.apis.abstract
from connexion.operation import Operation
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

class OperationWithAuthorizer(Operation):
    def oauth2_authorize(self, function):
        def wrapper(request):
            if "token_info" in request.context.values:
                token_info = request.context.values["token_info"]
                authorized_domains = os.environ["AUTHORIZED_DOMAINS"].split()
                assert int(token_info["expires_in"]) > 0
                assert json.loads(token_info["email_verified"]) is True
                assert any(token_info["email"].endswith(f"@{ad}") for ad in authorized_domains)
            return function(request)
        return wrapper

    def security_decorator(self, function):
        return super().security_decorator(self.oauth2_authorize(function))

connexion.apis.abstract.Operation = OperationWithAuthorizer

@failsafe
def create_app():
    app = connexion.App(__name__)
    resolver = RestyResolver("dss.api", collection_endpoint_name="list")
    app.add_api('../dss-api.yml', resolver=resolver, validate_responses=True, arguments=os.environ)
    return app

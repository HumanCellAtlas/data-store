#!/usr/bin/env python

"""
DSS description FIXME: elaborate
"""
import traceback

import os
import json
import logging

import connexion
import flask
import requests
import connexion.apis.abstract
from connexion.lifecycle import ConnexionResponse
from connexion.operation import Operation
from connexion.resolver import RestyResolver
from connexion.exceptions import OAuthProblem, OAuthResponseProblem, OAuthScopeProblem
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
        problem = {
            'status': requests.codes.server_error,
            'code': "unhandled_exception",
            'title': str(exception),
            'stacktrace': traceback.format_exc(),
        }
        if isinstance(exception, (OAuthProblem, OAuthResponseProblem, OAuthScopeProblem)):
            problem['status'] = exception.code
            problem['code'] = exception.__class__.__name__
            problem['title'] = exception.description
        return ConnexionResponse(
            status_code=problem['status'],
            mimetype="application/problem+json",
            content_type="application/problem+json",
            body=problem,
        )

class OperationWithAuthorizer(Operation):
    authorized_domains = os.environ["AUTHORIZED_DOMAINS"].split()
    def oauth2_authorize(self, function):
        def wrapper(request):
            if "token_info" in request.context.values:
                token_info = request.context.values["token_info"]
                if not int(token_info["expires_in"]) > 0:
                    raise OAuthProblem(description="Authorization token has expired")
                if json.loads(token_info["email_verified"]) is not True:
                    raise OAuthProblem(description="User email is unverified")
                if not any(token_info["email"].endswith(f"@{ad}") for ad in self.authorized_domains):
                    raise OAuthProblem(description="User email is unauthorized")
            return function(request)
        return wrapper

    def security_decorator(self, function):
        return super().security_decorator(self.oauth2_authorize(function))

connexion.apis.abstract.Operation = OperationWithAuthorizer

@failsafe
def create_app():
    app = DSSApp(__name__)
    resolver = RestyResolver("dss.api", collection_endpoint_name="list")
    app.add_api('../dss-api.yml', resolver=resolver, validate_responses=True, arguments=os.environ)
    return app

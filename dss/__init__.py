#!/usr/bin/env python

"""
DSS description FIXME: elaborate
"""

import logging

import flask
import connexion
from connexion.resolver import RestyResolver
from flask_failsafe import failsafe

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

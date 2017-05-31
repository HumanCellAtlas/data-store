#!/usr/bin/env python

"""
DSS description FIXME: elaborate
"""

import flask
import connexion
from connexion.resolver import RestyResolver
from flask_failsafe import failsafe

def get_logger():
    return flask.current_app.logger

@failsafe
def create_app():
    app = connexion.App(__name__)
    resolver = RestyResolver("dss.api", collection_endpoint_name="list")
    app.add_api('../dss-api.yml', resolver=resolver, validate_responses=True)
    return app

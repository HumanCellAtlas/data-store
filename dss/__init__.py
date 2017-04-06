#!/usr/bin/env python

"""
DSS description FIXME: elaborate
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os, sys, json, time, logging
from datetime import datetime, timedelta

import connexion
from connexion.resolver import RestyResolver

logging.basicConfig(level=logging.DEBUG)

app = connexion.App(__name__)
resolver = RestyResolver("dss.api", collection_endpoint_name="list")
app.add_api('../dss-api.yml', resolver=resolver, validate_responses=True)

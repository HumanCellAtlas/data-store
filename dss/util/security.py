#!/usr/bin/env python3.6
import base64
import json
import logging

import functools
import jwt
import requests
import typing
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from flask import request

from dss import Config
from dss.error import DSSForbiddenException, DSSException
from dss.util.auth import Auth

logger = logging.getLogger(__name__)

# recycling the same session for all requests.
session = requests.Session()




def security_assert():
    def real_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # create auth instance\
            auth_handler = Auth(
            auth_handler.security_flow(request.token_info)
            # perform auth mapping
            return func(*args, **kwargs)

        return wrapper

    return real_decorator
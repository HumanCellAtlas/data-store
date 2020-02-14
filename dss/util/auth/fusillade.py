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


class Fusillade(Auth):
    def __init__(self):

    # TODO create mapping functionality here

    # this function is to be deleted, place it into the flow mapping
    def authorized_group_required(groups: typing.List[str]):
        def real_decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                super().assert_authorized_group(groups, request.token_info)
                return func(*args, **kwargs)

            return wrapper

        return real_decorator

    def assert_authorized(principal: str,
                          actions: typing.List[str],
                          resources: typing.List[str]):
        resp = super().session.post(f"{Config.get_authz_url()}/v1/policies/evaluate",
                            headers=Config.get_ServiceAccountManager().get_authorization_header(),
                            json={"action": actions,
                                  "resource": resources,
                                  "principal": principal})
        resp.raise_for_status()
        resp_json = resp.json()
        if not resp_json.get('result'):
            raise DSSForbiddenException(title=f"User is not authorized to access this resource:\n{resp_json}")

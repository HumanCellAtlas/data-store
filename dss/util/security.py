#!/usr/bin/env python3.6
import base64
import functools
import json
import logging
import os
import typing

import jwt
import requests
import time
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from flask import request

from dss import Config
from dss.error import DSSForbiddenException, DSSException
from dss.util import get_gcp_credentials_file

logger = logging.getLogger(__name__)

allowed_algorithms = ['RS256']
gserviceaccount_domain = "iam.gserviceaccount.com"

# recycling the same session for all requests.
session = requests.Session()


@functools.lru_cache(maxsize=32)
def get_openid_config(openid_provider):
    res = session.get(f"{openid_provider}.well-known/openid-configuration")
    res.raise_for_status()
    return res.json()


def get_jwks_uri(openid_provider):
    if openid_provider.endswith(gserviceaccount_domain):
        return f"https://www.googleapis.com/service_accounts/v1/jwk/{openid_provider}"
    else:
        return get_openid_config(openid_provider)["jwks_uri"]


@functools.lru_cache(maxsize=32)
def get_public_keys(openid_provider):
    keys = session.get(get_jwks_uri(openid_provider)).json()["keys"]
    return {
        key["kid"]: rsa.RSAPublicNumbers(
            e=int.from_bytes(base64.urlsafe_b64decode(key["e"] + "==="), byteorder="big"),
            n=int.from_bytes(base64.urlsafe_b64decode(key["n"] + "==="), byteorder="big")
        ).public_key(backend=default_backend())
        for key in keys
    }


def verify_jwt(token: str) -> typing.Optional[typing.Mapping]:
    try:
        unverified_token = jwt.decode(token, verify=False)
    except jwt.DecodeError:
        logger.info(f"Failed to decode JWT: {token}", exc_info=True)
        raise DSSException(401, 'Unauthorized', 'Failed to decode token.')

    assert_authorized_issuer(unverified_token)
    issuer = unverified_token['iss']
    public_keys = get_public_keys(issuer)

    try:
        token_header = jwt.get_unverified_header(token)
        verified_tok = jwt.decode(token,
                                  key=public_keys[token_header["kid"]],
                                  issuer=issuer,
                                  audience=Config.get_audience(),
                                  algorithms=allowed_algorithms,
                                  )
        logger.info("""{"valid": true, "token": %s}""", json.dumps(verified_tok))
    except jwt.PyJWTError as ex:  # type: ignore
        logger.info("""{"valid": false, "token": %s}""", json.dumps(unverified_token), exc_info=True)
        raise DSSException(401, 'Unauthorized', 'Authorization token is invalid') from ex
    return verified_tok


def get_token_email(token_info: typing.Mapping[str, typing.Any]) -> str:
    try:
        email_claim = Config.get_OIDC_email_claim()
        return token_info.get(email_claim) or token_info['email']
    except KeyError:
        raise DSSException(401, 'Unauthorized', 'Authorization token is missing email claims.')


def assert_authorized_issuer(token: typing.Mapping[str, typing.Any]) -> None:
    """
    Must be either `Config.get_openid_provider()` or in `Config.get_trusted_google_projects()`
    :param token: dict
    """
    issuer = token['iss']
    if issuer == Config.get_openid_provider():
        return
    service_name, _, service_domain = issuer.partition("@")
    if service_domain in Config.get_trusted_google_projects() and issuer == token['sub']:
        return
    logger.info(f"Token issuer not authorized: {issuer}")
    raise DSSForbiddenException()


def assert_authorized_group(group: typing.List[str], token: dict) -> None:
    if token.get(Config.get_OIDC_group_claim()) in group:
        return
    logger.info(f"User not in authorized group: {group}, {token}")
    raise DSSForbiddenException()


def authorized_group_required(groups: typing.List[str]):
    def real_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            assert_authorized_group(groups, request.token_info)
            return func(*args, **kwargs)
        return wrapper
    return real_decorator


def assert_authorized(principal: str,
                      actions: typing.List[str],
                      resources: typing.List[str]):
    resp = session.post(f"{Config.get_authz_url()}/v1/policies/evaluate",
                        headers=DSS_AUTHZ.get_header(),
                        json={"action": actions,
                              "resource": resources,
                              "principal": principal})
    if not resp.json()['result']:
        raise DSSForbiddenException()


class DSS_AUTHZ:
    _token = None
    _renew_buffer = 30  # If the token will expire in less than this amount of time in seconds, generate a new token.
    _lifetime = 3600  # How long the token should live.

    @classmethod
    def get_header(cls):
        return {"Authorization": f"Bearer {cls.get_service_jwt()}"}

    @classmethod
    def get_service_jwt(cls):
        if cls._token is None or cls.is_expired():
            # create a JWT
            if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = get_gcp_credentials_file().name
            with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], "r") as fh:
                service_credentials = json.loads(fh.read())

            iat = time.time()
            exp = iat + cls._lifetime
            payload = {'iss': service_credentials["client_email"],
                       'sub': service_credentials["client_email"],
                       'aud': Config.get_audience(),
                       'iat': iat,
                       'exp': exp,
                       'scope': ['email', 'openid', 'offline_access']
                       }
            additional_headers = {'kid': service_credentials["private_key_id"]}
            cls._token = jwt.encode(payload, service_credentials["private_key"], headers=additional_headers,
                                    algorithm='RS256').decode()
        return cls._token

    @classmethod
    def is_expired(cls):
        try:
            decoded_token = jwt.decode(cls._token, options={'verify_signature': False})
        except jwt.ExpiredSignatureError:  # type: ignore
            return True
        else:
            if decoded_token['exp'] - time.time() <= cls._renew_buffer:
                # If the token will expire in less than cls._renew_buffer amount of time in seconds, the token is
                # considered expired.
                return True
            else:
                return False

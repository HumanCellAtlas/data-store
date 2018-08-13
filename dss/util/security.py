#!/usr/bin/env python3.6
import os, functools, base64, typing

import requests
import jwt
import logging

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

from flask import request

from dss import Config
from dss.error import DSSForbiddenException, DSSException

logger = logging.getLogger(__name__)

allowed_algorithms = ['RS256']
gserviceaccount = "iam.gserviceaccount.com"

# using a connection pool for token_info, and
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
session = requests.Session()
session.mount('http://', adapter)
session.mount('https://', adapter)


@functools.lru_cache(maxsize=32)
def get_openid_config(openid_provider):
    res = requests.get(f"{openid_provider}.well-known/openid-configuration")
    res.raise_for_status()
    return res.json()


def get_jwks_uri(openid_provider):
    if openid_provider.endswith(gserviceaccount):
        return f"https://www.googleapis.com/service_accounts/v1/jwk/{openid_provider}"
    else:
        return get_openid_config(openid_provider)["jwks_uri"]


@functools.lru_cache(maxsize=32)
def get_public_keys(openid_provider):
    keys = requests.get(get_jwks_uri(openid_provider)).json()["keys"]
    return {
        key["kid"]: rsa.RSAPublicNumbers(
            e=int.from_bytes(base64.urlsafe_b64decode(key["e"] + "==="), byteorder="big"),
            n=int.from_bytes(base64.urlsafe_b64decode(key["n"] + "==="), byteorder="big")
        ).public_key(backend=default_backend())
        for key in keys
    }


def get_token_info(token):
    verified_token = verify_jwt(token)
    if Config.get_token_info_url() in verified_token['aud']:
        token_request = session.get(Config.get_token_info_url(),
                                    headers={'Authorization': 'Bearer {}'.format(token)},
                                    timeout=5)
        verified_token.update(token_request.json())
        if verified_token.get('email_verified') is not True:
            logger.info("Email not verified: {verified_token}")
            raise DSSForbiddenException()
    else:
        # then token is a google service credential from a trusted google project.
        verified_token['email'] = verified_token['sub']
        if not (verified_token.get('scope') and verified_token.get('scopes')):
            verified_token['scopes'] = []
    return verified_token


def verify_jwt(token: str) -> typing.Optional[typing.Mapping]:
    try:
        unverified_token = jwt.decode(token, verify=False)
        issuer = unverified_token["iss"]
        is_authorized_issuer(issuer)
        public_keys = get_public_keys(issuer)
        token_header = jwt.get_unverified_header(token)
        if Config.deployment_stage() != 'prod':
            logging.info(f"Token expiration is not checked in non production stages. Token: {token}")
            tok = jwt.decode(token,
                             key=public_keys[token_header["kid"]],
                             issuer=issuer,
                             audience=Config.get_audience(),
                             algorithms=allowed_algorithms,
                             options={'verify_exp': False})
        else:
            tok = jwt.decode(token,
                             key=public_keys[token_header["kid"]],
                             issuer=issuer,
                             audience=Config.get_audience(),
                             algorithms=allowed_algorithms,
                             )
    except jwt.PyJWTError as ex:  # type: ignore
        logger.info(f"JWT failed to validate.", exc_info=True)
        raise DSSException(401, 'Unauthorized', 'Authorization token is invalid') from ex
    return tok


def is_authorized_issuer(issuer: str) -> None:
    """
    Must be either "https://humancellatlas.auth0.com/" or service credential from a trusted google project.
    :param issuer: str
    """
    if issuer != Config.get_openid_provider() and not any(issuer.endswith(f"@{p}")
                                                          for p in Config.get_trusted_google_projects()):
        logger.info(f"Token issuer not authorized: {issuer}")
        raise DSSForbiddenException()


def is_authorized_group(group: typing.List[str], token_info: dict) -> None:
    if token_info.get("https://auth.data.humancellatlas.org/group") in group:
        return
    if token_info.get("sub", '').endswith(gserviceaccount):
        return
    logger.info(f"User not in authorized group: {group}, {token_info}")
    raise DSSForbiddenException()


def authorized_group_required(groups: typing.List[str]):
    def real_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            is_authorized_group(groups, request.token_info)
            return func(*args, **kwargs)
        return wrapper
    return real_decorator

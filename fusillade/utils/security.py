#!/usr/bin/env python3.6
"""
Used by connexion to verify the JWT in Authorization header of the request.
"""
import json
import functools, base64, typing

import requests
import jwt
import logging

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

from furl import furl

from fusillade import Config
from fusillade.errors import FusilladeHTTPException

logger = logging.getLogger(__name__)

allowed_algorithms = ['RS256']
gserviceaccount_domain = "iam.gserviceaccount.com"

# recycling the same session for all requests.
session = requests.Session()


@functools.lru_cache(maxsize=32)
def get_openid_config(openid_provider=None):
    """

    :param openid_provider: the openid provider's domain.
    :return: the openid configuration
    """
    if not openid_provider:
        openid_provider = Config.get_openid_provider()
    elif openid_provider.endswith(gserviceaccount_domain):
        openid_provider = 'accounts.google.com'
    elif openid_provider.startswith("https://"):
        openid_provider = furl(openid_provider).host
    res = requests.get(f"https://{openid_provider}/.well-known/openid-configuration")
    res.raise_for_status()
    return res.json()


def get_jwks_uri(openid_provider):
    if openid_provider.endswith(gserviceaccount_domain):
        return f"https://www.googleapis.com/service_accounts/v1/jwk/{openid_provider}"
    else:
        return get_openid_config(openid_provider)["jwks_uri"]


@functools.lru_cache(maxsize=32)
def get_public_keys(openid_provider):
    """
    Fetches the public key from an OIDC Identity provider to verify the JWT.
    :param openid_provider: the openid provider's domain.
    :return: Public Keys
    """
    keys = session.get(get_jwks_uri(openid_provider)).json()["keys"]
    return {
        key["kid"]: rsa.RSAPublicNumbers(
            e=int.from_bytes(base64.urlsafe_b64decode(key["e"] + "==="), byteorder="big"),
            n=int.from_bytes(base64.urlsafe_b64decode(key["n"] + "==="), byteorder="big")
        ).public_key(backend=default_backend())
        for key in keys
    }


def verify_jwt(token: str) -> typing.Optional[typing.Mapping]:
    """
    Verify the JWT from the request. This is function is referenced in fusillade-api.yml
    securitySchemes.BearerAuth.x-bearerInfoFunc. It's used by connexion to authorize api endpoints that use BearAuth
    securitySchema.

    :param token: the Authorization header in the request.
    :return: Decoded and verified token.
    """
    try:
        unverified_token = jwt.decode(token, verify=False)
    except jwt.DecodeError:
        logger.debug('{"msg": "Failed to decode token."}', exc_info=True)
        raise FusilladeHTTPException(401, 'Unauthorized', 'Failed to decode token.')

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
        logger.debug("""{"msg": "Token Validated"}""")
    except jwt.PyJWTError as ex:  # type: ignore
        logger.debug("""{"msg": "Failed to validate token."}""", exc_info=True)
        raise FusilladeHTTPException(401, 'Unauthorized', 'Authorization token is invalid') from ex
    return verified_tok

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


logger = logging.getLogger(__name__)


allowed_algorithms = ['RS256']
gserviceaccount_domain = "iam.gserviceaccount.com"

class Auth:
    def __init__(self, session: requests.Session):
        self.session = session

    # TODO what does this base function operate like? We want to abstract all the information
    # about the other child functions that derive from here....
    # set the ENV var and forge about it! :)

    # name this better
    def security_flow(*args, **kwargs):
        """
        This function maps out flow for a given security config
        """

        # look at how the oeprations factory use
        #
        # def process_keys(self):
        #     raise NotImplementedError()
        #
        # def __call__(self, argv: typing.List[str], args: argparse.Namespace):
        #     self.process_keys()

        # there has to be a way to replicate this to call the applicable security flow, perhaps the
        # hca blob-store has more of an example on how to perform this 

    @functools.lru_cache(maxsize=32)
    def get_openid_config(openid_provider):
        res = self.session.get(f"{openid_provider}.well-known/openid-configuration")
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


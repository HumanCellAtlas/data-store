#!/usr/bin/env python3.6
import functools
import typing
from flask import request

from dss.util.auth import AuthHandler
from dss.util.auth.helpers import verify_jwt


def assert_security(groups: typing.List[str]):
    def real_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # connexion at this point has verified the jwt using TOKENINFO_FUNC
            authz_handler = AuthHandler()
            authz_handler.security_flow(authz_methods=['groups'], groups=groups, token=request.token_info)
            return func(*args, **kwargs)

        return wrapper

    return real_decorator

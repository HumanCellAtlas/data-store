#!/usr/bin/env python3.6
import functools


from dss.util.auth import AuthHandler
from dss.util.auth.helpers import verify_jwt


def security_assert(*args, **kwargs):
    def real_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            authz_handler = AuthHandler()  # this function returns a class derived from Authorization
            authz_handler.security_flow(kwargs)
            # perform auth mapping
            return func(*args, **kwargs)

        return wrapper

    return real_decorator

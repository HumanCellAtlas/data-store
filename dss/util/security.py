#!/usr/bin/env python3.6
import functools
from flask import request

from dss.util.authorize import Authorize
from dss.util.authenticate.helpers import verify_jwt

# is it possible to create something here that can have different auth mechanisms?
# can this information be handed to the system from the wrapper?
# security handlers in functions are not that bad, it provides a single location to change
# what is happening in the system.


def security_assert():
    def real_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # verify jwt being passed.
            verify_jwt(request.token_info)
            authz_handler = Authorize()
            authz_handler.security_flow(request.token_info)
            # perform auth mapping
            return func(*args, **kwargs)

        return wrapper

    return real_decorator

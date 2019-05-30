import json
import os
import logging
import re
import functools
import traceback

import requests
import werkzeug.exceptions
from connexion.lifecycle import ConnexionResponse
from flask import request
from flask import Response as FlaskResponse


logger = logging.getLogger(__name__)


class DSSException(Exception):
    def __init__(self, status: int, code: str, title: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)  # type: ignore
        self.status = status
        self.code = code
        self.message = title


class DSSBindingException(DSSException):
    def __init__(self, title, *args, **kwargs) -> None:
        super().__init__(requests.codes.bad_request, "illegal_arguments", title, *args, **kwargs)


class DSSForbiddenException(DSSException):
    def __init__(self, title: str = "User is not authorized to access this resource",
                 *args, **kwargs) -> None:
        super().__init__(requests.codes.forbidden,
                         "Forbidden",
                         title,
                         *args, **kwargs)


def include_retry_after_header(return_code, method, uri):
    uuid_pattern = '[A-Za-z0-9]{8}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{12}'
    bundle_checkout_pattern = f'/v1/bundles/{uuid_pattern}/checkout'
    bundle_checkout = re.compile(bundle_checkout_pattern)

    # we do not include Retry-After headers for these return codes and API endpoints
    exclusion_list = [(requests.codes.server_error,        'POST', bundle_checkout),  # noqa
                      (requests.codes.bad_gateway,         'POST', bundle_checkout),  # noqa
                      (requests.codes.service_unavailable, 'POST', bundle_checkout),  # noqa
                      (requests.codes.gateway_timeout,     'POST', bundle_checkout)]  # noqa

    for excluded_call in exclusion_list:
        if excluded_call[0] == return_code and \
           excluded_call[1] == method and \
           excluded_call[2].match(uri):
            return False
    return True


def dss_exception_handler(e: DSSException) -> FlaskResponse:
    return FlaskResponse(
        status=e.status,
        mimetype="application/problem+json",
        content_type="application/problem+json",
        response=json.dumps({
            'status': e.status,
            'code': e.code,
            'title': e.message,
            'stacktrace': traceback.format_exc(),
        }))


def dss_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if (os.environ.get('DSS_READ_ONLY_MODE') is None
                or "GET" == request.method
                or ("POST" == request.method and "search" in request.path)):
            try:
                return func(*args, **kwargs)
            except werkzeug.exceptions.HTTPException as ex:
                status = ex.code
                code = ex.name
                title = str(ex)
                stacktrace = traceback.format_exc()
                headers = None
            except DSSException as ex:
                status = ex.status
                code = ex.code
                title = ex.message
                stacktrace = traceback.format_exc()
                headers = None
            except Exception as ex:
                status = requests.codes.server_error
                code = "unhandled_exception"
                title = str(ex)
                stacktrace = traceback.format_exc()
                headers = None
            logger.error(stacktrace)
        else:
            status = requests.codes.unavailable
            code = "read_only"
            title = "The DSS is currently read-only"
            stacktrace = ""
            headers = {'Retry-After': '600'}

        if include_retry_after_header(return_code=status, method=request.method, uri=request.path):
            headers = {'Retry-After': '10'}

        return ConnexionResponse(
            status_code=status,
            mimetype="application/problem+json",
            content_type="application/problem+json",
            headers=headers,
            body={
                'status': status,
                'code': code,
                'title': title,
                'stacktrace': stacktrace,
            })

    return wrapper

import json
import os
import sys
import logging
import functools
import traceback
import requests
import werkzeug.exceptions
from connexion.lifecycle import ConnexionResponse
from flask import request
from flask import Response as FlaskResponse

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.storage.identifiers import FILES_URI_REGEX, BUNDLES_URI_REGEX, ROOT_SEARCH_URI_REGEX, BUNDLE_CHECKOUT_URI_REGEX
from dss.storage.identifiers import SUBSCRIPTIONS_URI_REGEX, ROOT_SUBSCRIPTIONS_URI_REGEX
from dss.storage.identifiers import COLLECTIONS_URI_REGEX, ROOT_COLLECTIONS_URI_REGEX
from dss.storage.identifiers import MOCK_PATTERN, MOCK_URI_REGEX

MOCK_METHOD = 'GET'
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
    # include Retry-After headers for these API endpoints
    inclusion_list = [('GET', BUNDLE_CHECKOUT_URI_REGEX),

                      ('GET', BUNDLES_URI_REGEX),
                      ('DELETE', BUNDLES_URI_REGEX),
                      ('PATCH', BUNDLES_URI_REGEX),
                      ('PUT', BUNDLES_URI_REGEX),

                      ('GET', ROOT_COLLECTIONS_URI_REGEX),
                      ('PUT', ROOT_COLLECTIONS_URI_REGEX),
                      ('DELETE', COLLECTIONS_URI_REGEX),
                      ('GET', COLLECTIONS_URI_REGEX),
                      ('PATCH', COLLECTIONS_URI_REGEX),

                      ('GET', FILES_URI_REGEX),
                      ('HEAD', FILES_URI_REGEX),
                      ('PUT', FILES_URI_REGEX),

                      ('POST', ROOT_SEARCH_URI_REGEX),

                      ('GET', ROOT_SUBSCRIPTIONS_URI_REGEX),
                      ('PUT', ROOT_SUBSCRIPTIONS_URI_REGEX),
                      ('DELETE', SUBSCRIPTIONS_URI_REGEX),
                      ('GET', SUBSCRIPTIONS_URI_REGEX),

                      (MOCK_METHOD, MOCK_URI_REGEX)]

    # we only include Retry-After headers for these return codes
    retry_after_codes = [requests.codes.server_error,
                         requests.codes.bad_gateway,
                         requests.codes.service_unavailable,
                         requests.codes.gateway_timeout]

    if return_code in retry_after_codes:
        for api_call in inclusion_list:
            if method == api_call[0] and api_call[1].match(uri):
                return True
    return False


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


def determine_method_path(func):
    if 'mock' in func.__name__:
        return MOCK_METHOD, MOCK_PATTERN
    return request.method, request.path


def dss_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        method, path = determine_method_path(func)
        if os.environ.get('DSS_READ_ONLY_MODE') is None or "GET" == method or ("POST" == method and "search" in path):
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

        if include_retry_after_header(return_code=status, method=method, uri=path):
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

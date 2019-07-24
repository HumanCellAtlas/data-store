import json
import os
import logging
import functools
import traceback
import requests
import werkzeug.exceptions
from urllib3.util.retry import Retry
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
    # include Retry-After headers for these methods (all are considered idempotent)
    inclusion_list = Retry.DEFAULT_METHOD_WHITELIST

    # we only include Retry-After headers for these return codes
    retry_after_codes = [requests.codes.server_error,
                         requests.codes.bad_gateway,
                         requests.codes.service_unavailable,
                         requests.codes.gateway_timeout]

    if return_code in retry_after_codes and (method in inclusion_list or ("POST" == method and "search" in uri)):
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


def dss_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        method, path = request.method, request.path
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
            headers = {'Retry-After': 600}

        if include_retry_after_header(return_code=status, method=method, uri=path):
            headers = {'Retry-After': 10}

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

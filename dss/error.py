import json
import os
import random
import logging

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


def maybe_fake_error(headers, code) -> bool:
    # sometimes the capitalization gets a little funky when the headers come out the other end
    probability = headers.get(f"DSS_FAKE_{code}_PROBABILITY") or headers.get(f"Dss_Fake_{code}_Probability") or "0.0"

    try:
        fake_error_probability = float(probability)
    except ValueError:
        return None

    if random.random() > fake_error_probability:
        return None

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
        if maybe_fake_error(headers=request.headers, code=500):
            status = 500
            code = 'unhandled_exception'
            title = 'Unhandled Exception'
            stacktrace = 'This is a fake error used for testing.'
        elif maybe_fake_error(headers=request.headers, code=502):
            status = 502
            code = 'bad_gateway'
            title = 'Bad Gateway'
            stacktrace = 'This is a fake error used for testing.'
        elif maybe_fake_error(headers=request.headers, code=503):
            status = 503
            code = 'service_unavailable'
            title = 'Service Unavailable'
            stacktrace = 'This is a fake error used for testing.'
        # fake/real 504 responses are raised via a similar mechanic in data-store/chalice/app.py
        elif (os.environ.get('DSS_READ_ONLY_MODE') is None
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
            headers = {'Retry-After': 600}

        # These errors may be returned by the chalice app before reaching here.
        if status in (requests.codes.server_error,         # 500 status code
                      requests.codes.bad_gateway,          # 502 status code
                      requests.codes.service_unavailable,  # 503 status code
                      requests.codes.gateway_timeout):     # 504 status code
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

import typing

import functools
import traceback

import requests
import werkzeug.exceptions
from connexion.lifecycle import ConnexionResponse
from flask import Response as FlaskResponse


class DSSException(Exception):
    def __init__(self, status: int, code: str, title: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.status = status
        self.code = code
        self.message = title


class DSSBindingException(DSSException):
    def __init__(self, title, *args, **kwargs) -> None:
        super().__init__(requests.codes.bad_request, "illegal_arguments", title, *args, **kwargs)


class DSSForbiddenException(DSSException):
    def __init__(self, title: str="User is not authorized to access this resource",
                 *args, **kwargs) -> None:
        super().__init__(requests.codes.forbidden,
                         "Forbidden",
                         title,
                         *args, **kwargs)

def handler_DSSException(e: DSSException) -> FlaskResponse:
    return FlaskResponse(
        status=e.status,
        mimetype="application/problem+json",
        content_type="application/problem+json",
        response={
            'status': e.status,
            'code': e.code,
            'title': e.message,
            'stacktrace': traceback.format_exc(),
        })


def dss_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except werkzeug.exceptions.HTTPException as ex:
            status = ex.code
            code = ex.name
            title = str(ex)
            stacktrace = traceback.format_exc()
        except DSSException as ex:
            status = ex.status
            code = ex.code
            title = ex.message
            stacktrace = traceback.format_exc()
        except Exception as ex:
            status = requests.codes.server_error
            code = "unhandled_exception"
            title = str(ex)
            stacktrace = traceback.format_exc()

        return ConnexionResponse(
            status_code=status,
            mimetype="application/problem+json",
            content_type="application/problem+json",
            body={
                'status': status,
                'code': code,
                'title': title,
                'stacktrace': stacktrace,
            })

    return wrapper

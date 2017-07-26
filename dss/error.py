import traceback

import flask
import functools
import requests
import werkzeug.exceptions
from connexion.lifecycle import ConnexionResponse


class DSSException(Exception):
    def __init__(self, status: int, code: str, title: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.status = status
        self.code = code
        self.message = title


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

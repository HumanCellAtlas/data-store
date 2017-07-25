import traceback

import flask
import functools
import requests
import werkzeug.exceptions


class DSSException(Exception):
    def __init__(self, http_error_code: int, code: str, message: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.http_error_code = http_error_code
        self.code = code
        self.message = message


def dss_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except werkzeug.exceptions.HTTPException as ex:
            http_error_code = ex.code
            code = ex.name
            message = str(ex)
            stacktrace = traceback.format_exc()
        except DSSException as ex:
            http_error_code = ex.http_error_code
            code = ex.code
            message = ex.message
            stacktrace = traceback.format_exc()
        except Exception as ex:
            http_error_code = requests.codes.server_error
            code = "unhandled_exception"
            message = str(ex)
            stacktrace = traceback.format_exc()

        return (
            flask.jsonify({
                'http-error-code': http_error_code,
                'code': code,
                'message': message,
                'stacktrace': stacktrace,
            }),
            http_error_code
        )

    return wrapper

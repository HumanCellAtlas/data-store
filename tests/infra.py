import os
import sys
import json
import logging
import re
import typing
import urllib.parse

import functools
from flask import wrappers


def start_verbose_logging():
    logging.basicConfig(level=logging.INFO)
    for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
        if logger_name.startswith("botocore") or logger_name.startswith("boto3.resources"):
            logging.getLogger(logger_name).setLevel(logging.WARNING)


def progress(message):
    """
    Print progress commentary if env 'VERBOSE' is set to anything.

    Note it doesn't include a newline so you can produce dots... or "doing...done" lines.
    """
    if 'VERBOSE' in os.environ:
        sys.stdout.write(message)
        sys.stdout.flush()


class DSSAsserts(object):
    def setup(self):
        self.sre = re.compile("^assert(.+)Response")

    def assertResponse(
            self,
            method: str,
            path: str,
            expected_code: int,
            json_request_body: typing.Optional[dict]=None,
            **kwargs) -> typing.Tuple[wrappers.Response, str, typing.Optional[dict]]:
        """
        Make a request given a HTTP method and a path.  The HTTP status code is checked against `expected_code`.

        If json_request_body is provided, it is serialized and set as the request body, and the content-type of the
        request is set to application/json.

        The first element of the return value is the response object.  The second element of the return value is the
        response text.

        If `parse_response_as_json` is true, then attempt to parse the response body as JSON and return that as the
        third element of the return value.  Otherwise, the third element of the return value is None.
        """
        if json_request_body is not None:
            if 'data' in kwargs:
                self.fail("both json_input and data are defined")
            kwargs['data'] = json.dumps(json_request_body)
            kwargs['content_type'] = 'application/json'

        response = getattr(self.app, method)(path, **kwargs)
        self.assertEqual(response.status_code, expected_code)

        try:
            actual_json = json.loads(response.data.decode("utf-8"))
        except Exception:
            actual_json = None
        return response, response.data, actual_json

    def assertHeaders(
            self,
            response: wrappers.Response,
            expected_headers: dict = {}) -> None:
        for header_name, header_value in expected_headers.items():
            self.assertEqual(response.headers[header_name], header_value)

    # this allows for assert*Response, where * = the request method.
    def __getattr__(self, item: str) -> typing.Any:
        if item.startswith("assert"):
            mo = self.sre.match(item)
            if mo is not None:
                method = mo.group(1).lower()
                return functools.partial(self.assertResponse, method)

        if hasattr(super(DSSAsserts, self), '__getattr__'):
            return super(DSSAsserts, self).__getattr__(item)  # type: ignore
        else:
            raise AttributeError(item)


class UrlBuilder(object):
    def __init__(self):
        self.splitted = urllib.parse.SplitResult("", "", "", "", "")
        self.query = list()

    def set(self, scheme: str=None, netloc: str=None, path: str=None, fragment: str=None) -> "UrlBuilder":
        kwargs = dict()
        if scheme is not None:
            kwargs['scheme'] = scheme
        if netloc is not None:
            kwargs['netloc'] = netloc
        if path is not None:
            kwargs['path'] = path
        if fragment is not None:
            kwargs['fragment'] = fragment
        self.splitted = self.splitted._replace(**kwargs)

        return self

    def add_query(self, query_name: str, query_value: str) -> "UrlBuilder":
        self.query.append((query_name, query_value))

        return self

    def __str__(self) -> str:
        result = self.splitted._replace(query=urllib.parse.urlencode(self.query, doseq=True))

        return urllib.parse.urlunsplit(result)

import functools
import json
import logging
import os
import re
import typing

from flask import wrappers


def start_verbose_logging():
    logging.basicConfig(level=logging.INFO)
    for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
        if logger_name.startswith("botocore") or logger_name.startswith("boto3.resources"):
            logging.getLogger(logger_name).setLevel(logging.WARNING)


def get_env(varname):
    if varname not in os.environ:
        raise RuntimeError(
            "Please set the {} environment variable".format(varname))
    return os.environ[varname]


class ExpectedErrorFields(typing.NamedTuple):
    code: str

    status: typing.Optional[int] = None
    """
    If this is None, then we not check the status.  For all other values, we test that it matches.
    """

    expect_stacktrace: typing.Optional[bool] = None
    """
    If this is True, then we expect the stacktrace to be present.  If this is False, then we expect the stacktrace to be
    absent.  If this is None, then we do not test the presence of the stacktrace.
    """


class DSSAssertResponse(typing.NamedTuple):
    response: wrappers.Response
    body: str
    json: typing.Optional[typing.Any]


class DSSAsserts:
    sre = re.compile("^assert(.+)Response")

    def assertResponse(
            self,
            method: str,
            path: str,
            expected_code: int,
            json_request_body: typing.Optional[dict]=None,
            expected_error: typing.Optional[ExpectedErrorFields]=None,
            **kwargs) -> DSSAssertResponse:
        """
        Make a request given a HTTP method and a path.  The HTTP status code is checked against `expected_code`.

        If json_request_body is provided, it is serialized and set as the request body, and the content-type of the
        request is set to application/json.

        The first element of the return value is the response object.  The second element of the return value is the
        response text.  Attempt to parse the response body as JSON and return that as the third element of the return
        value.  Otherwise, the third element of the return value is None.

        If expected_error is provided, the content-type is expected to be "application/problem+json" and the response is
        tested in accordance to the documentation of `ExpectedErrorFields`.
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

        if expected_error is not None:
            self.assertEqual(response.headers['content-type'], "application/problem+json")
            self.assertEqual(actual_json['code'], expected_error.code)
            self.assertIn('title', actual_json)
            if expected_error.status is not None:
                self.assertEqual(actual_json['status'], expected_error.status)
            if expected_error.expect_stacktrace is not None:
                self.assertEqual('stacktrace' in actual_json, expected_error.expect_stacktrace)

        return DSSAssertResponse(response, response.data, actual_json)

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

import json
import pprint
import typing

import collections.abc
import functools
import re

from flask import wrappers


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


class DSSAssertMixin:
    sre = re.compile("^assert(.+)Response")

    def assertResponse(
            self,
            method: str,
            path: str,
            expected_code: typing.Union[int, typing.Container[int]],
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
            if 'headers' not in kwargs:
                kwargs['headers'] = {}
            kwargs['headers']['Content-Type'] = "application/json"

        response = getattr(self.app, method)(path, **kwargs)
        try:
            actual_json = response.json()
        except json.decoder.JSONDecodeError:
            actual_json = None

        try:
            if isinstance(expected_code, collections.abc.Container):
                self.assertIn(response.status_code, expected_code)
            else:
                self.assertEqual(response.status_code, expected_code)

            if expected_error is not None:
                self.assertEqual(response.headers['content-type'], "application/problem+json")
                self.assertEqual(actual_json['code'], expected_error.code)
                self.assertIn('title', actual_json)
                if expected_error.status is not None:
                    self.assertEqual(actual_json['status'], expected_error.status)
                if expected_error.expect_stacktrace is not None:
                    self.assertEqual('stacktrace' in actual_json, expected_error.expect_stacktrace)
        except AssertionError:
            if actual_json is not None:
                print("Response:")
                pprint.pprint(actual_json)
            raise

        return DSSAssertResponse(response, response.content, actual_json)

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

        if hasattr(super(DSSAssertMixin, self), '__getattr__'):
            return super(DSSAssertMixin, self).__getattr__(item)  # type: ignore
        else:
            raise AttributeError(item)

    def assertRegexIn(self, expected_pattern: str, iterable: typing.List[str], msg: typing.Optional[str]=None) -> None:
        """Fails the test unless the expected_regex matches one of the strings in iterable"""
        if isinstance(expected_pattern, (str, bytes)):
            assert expected_pattern, "expected_regex must not be empty."
            expected_regex = re.compile(expected_pattern)
        for text in iterable:
            if expected_regex.search(text):
                return
        standard_msg = "Regex didn't match: %r not found in %r" % (expected_pattern, iterable)
        # _formatMessage ensures the longMessage option is respected
        msg = self._formatMessage(msg, standard_msg)
        raise self.failureException(msg)

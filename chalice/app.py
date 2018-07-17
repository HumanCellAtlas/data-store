import collections
import functools
import os
import random
import re
import sys
import threading
import time
import traceback
import typing
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import chalice
import nestedcontext
import requests
from flask import json

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import BucketConfig, Config, DeploymentStage, create_app
from dss.logging import configure_lambda_logging
from dss.util.tracing import DSS_XRAY_TRACE

if DSS_XRAY_TRACE:  # noqa
    from aws_xray_sdk.core import xray_recorder
    from aws_xray_sdk.core.context import Context
    from aws_xray_sdk.ext.flask.middleware import XRayMiddleware
    xray_recorder.configure(
        service='DSS',
        dynamic_naming=f"*{os.environ['API_DOMAIN_NAME']}*",
        context=Context(),
        context_missing='LOG_ERROR'
    )

configure_lambda_logging()

Config.set_config(BucketConfig.NORMAL)
Config.BLOBSTORE_CONNECT_TIMEOUT = 5
Config.BLOBSTORE_READ_TIMEOUT = 5
Config.BLOBSTORE_RETRIES = 2


EXECUTION_TERMINATION_THRESHOLD_SECONDS = 5.0
"""We will terminate execution if we have this many seconds left to process the request."""

API_GATEWAY_TIMEOUT_SECONDS = 30.0
"""
This is how quickly API Gateway gives up on Lambda.  This allows us to terminate the request if the lambda has a longer
timeout than API Gateway.
"""

OVERRIDE_EXECUTION_LIMIT_SECONDS = None
"""
This is how long we wait for a request, if set.  If the value is None, we try to use the lambda's timeout.
"""
DSS_VERSION = os.getenv('DSS_VERSION')
"""
Tag describing the version of the currently deployed DSS codebase.  Generated during deployment in the form:
[<latest-release-tag>-<commits-since-tag>-]<SHA>
"""


class DSSChaliceApp(chalice.Chalice):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._override_exptime_seconds = OVERRIDE_EXECUTION_LIMIT_SECONDS


def timeout_response() -> chalice.Response:
    """
    Produce a chalice Response object that indicates a timeout.  Stacktraces for all running threads, other than the
    current thread, are provided in the response object.
    """
    frames = sys._current_frames()
    current_threadid = threading.get_ident()
    trace_dump = {
        thread_id: traceback.format_stack(frame)
        for thread_id, frame in frames.items()
        if thread_id != current_threadid}

    problem = {
        'status': requests.codes.gateway_timeout,
        'code': "timed_out",
        'title': "Timed out processing request.",
        'traces': trace_dump,
    }
    return chalice.Response(
        status_code=problem['status'],
        headers={"Content-Type": "application/problem+json"},
        body=json.dumps(problem),
    )


def calculate_seconds_left(chalice_app: DSSChaliceApp) -> int:
    """
    Given a chalice app, return how much execution time is left, limited further by the API Gateway timeout.
    """
    time_remaining_s = min(
        API_GATEWAY_TIMEOUT_SECONDS,
        chalice_app.lambda_context.get_remaining_time_in_millis() / 1000)
    time_remaining_s = max(0.0, time_remaining_s - EXECUTION_TERMINATION_THRESHOLD_SECONDS)
    return time_remaining_s


def time_limited(chalice_app: DSSChaliceApp):
    """
    When this decorator is applied to a route handler, we will process the request in a secondary thread.  If the
    processing exceeds the time allowed, we will return a standardized error message.
    """
    def real_decorator(method: callable):
        @functools.wraps(method)
        def wrapper(*args, **kwargs):
            executor = ThreadPoolExecutor()
            try:
                future = executor.submit(method, *args, **kwargs)
                time_remaining_s = chalice_app._override_exptime_seconds  # type: typing.Optional[float]
                if time_remaining_s is None:
                    time_remaining_s = calculate_seconds_left(chalice_app)

                try:
                    chalice_response = future.result(timeout=time_remaining_s)
                    return chalice_response
                except TimeoutError:
                    return timeout_response()
            finally:
                executor.shutdown(wait=False)
        return wrapper
    return real_decorator


def get_chalice_app(flask_app) -> DSSChaliceApp:
    app = DSSChaliceApp(app_name=flask_app.name, configure_logs=False)

    @time_limited(app)
    def dispatch(*args, **kwargs):
        uri_params = app.current_request.uri_params or {}
        path_pattern = app.current_request.context["resourcePath"]
        path = path_pattern.format(**uri_params)
        method = app.current_request.method
        query_params = app.current_request.query_params
        req_body = app.current_request.raw_body if app.current_request._body is not None else None

        def maybe_fake_504() -> typing.Optional[chalice.Response]:
            fake_504_probability_str = app.current_request.headers.get("DSS_FAKE_504_PROBABILITY", "0.0")

            try:
                fake_504_probability = float(fake_504_probability_str)
            except ValueError:
                return None

            if random.random() > fake_504_probability:
                return None

            return timeout_response()

        if not DeploymentStage.IS_PROD():
            maybe_fake_504_result = maybe_fake_504()
            if maybe_fake_504_result is not None:
                return maybe_fake_504_result

        status_code = None
        try:
            with flask_app.test_request_context(
                    path=path,
                    base_url="https://{}".format(app.current_request.headers["host"]),
                    query_string=app.current_request.query_params,
                    method=app.current_request.method,
                    headers=list(app.current_request.headers.items()),
                    data=req_body,
                    environ_base=app.current_request.stage_vars):
                with nestedcontext.bind(
                        time_left=lambda: calculate_seconds_left(app),
                        skip_on_conflicts=True):
                    flask_res = flask_app.full_dispatch_request()
                    status_code = flask_res._status_code
        except Exception as e:
            app.log.exception('The request failed!')
        finally:
            app.log.info(
                "[dispatch] \"%s %s\" %s%s",
                method,
                path,
                str(status_code),
                ' ' + str(query_params) if query_params is not None else '',
            )

        # API Gateway/Cloudfront adds a duplicate Content-Length with a different value (not sure why)
        res_headers = dict(flask_res.headers)
        res_headers.pop("Content-Length", None)
        res_headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"

        return chalice.Response(status_code=status_code,
                                headers=res_headers,
                                body="".join([c.decode() if isinstance(c, bytes) else c for c in flask_res.response]))

    routes = collections.defaultdict(list)
    for rule in flask_app.url_map.iter_rules():
        routes[re.sub(r"<(.+?)(:.+?)?>", r"{\1}", rule.rule).rstrip("/")] += rule.methods
    for route, methods in routes.items():
        app.route(route, methods=list(set(methods) - {"OPTIONS"}), cors=True)(dispatch)

    with open(os.path.join(pkg_root, "index.html")) as fh:
        swagger_ui_html = fh.read()

    @app.route("/")
    @time_limited(app)
    def serve_swagger_ui():
        return chalice.Response(status_code=200,
                                headers={"Content-Type": "text/html"},
                                body=swagger_ui_html)

    @app.route("/version")
    @time_limited(app)
    def version():
        data = {
            'version_info': {
                'version': DSS_VERSION
            }
        }

        return chalice.Response(
            status_code=requests.codes.ok,
            headers={'Content-Type': "application/json"},
            body=data
        )

    @app.route("/internal/health")
    @time_limited(app)
    def health():
        return chalice.Response(status_code=200,
                                headers={"Content-Type": "text/html"},
                                body="OK")

    @app.route("/internal/slow_request", methods=["GET"])
    @time_limited(app)
    def slow_request():
        time.sleep(40)
        return chalice.Response(status_code=200,
                                headers={"Content-Type": "text/html"},
                                body="Slow request completed!")

    @app.route("/internal/notify", methods=["POST"])
    @time_limited(app)
    def handle_notification():
        event = app.current_request.json_body
        if event["kind"] == "storage#object" and event["selfLink"].startswith("https://www.googleapis.com/storage"):
            app.log.info("Ignoring Google Object Change Notification")
        else:
            raise NotImplementedError()

    @app.route("/internal/application_secrets", methods=["GET"])
    @time_limited(app)
    def get_application_secrets():
        application_secret_file = os.environ["GOOGLE_APPLICATION_SECRETS"]

        with open(application_secret_file, 'r') as fh:
            data = json.loads(fh.read())

        return chalice.Response(
            status_code=requests.codes.ok,
            headers={'Content-Type': "application/json", },
            body=data,
        )

    return app


dss_app = create_app()

if DSS_XRAY_TRACE:
    XRayMiddleware(dss_app.app, xray_recorder)

app = get_chalice_app(dss_app.app)

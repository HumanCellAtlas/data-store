import collections
import datetime
import functools
import logging
import os
import re
import sys
import threading
import time
import traceback
import typing
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import chalice
import boto3
import requests
from flask import json

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import DeploymentStage, Config, create_app
from dss.util import paginate


Config.set_config(DeploymentStage.NORMAL)


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
                    time_remaining_s = min(
                        API_GATEWAY_TIMEOUT_SECONDS,
                        chalice_app.lambda_context.get_remaining_time_in_millis() / 1000)
                    time_remaining_s = max(0.0, time_remaining_s - EXECUTION_TERMINATION_THRESHOLD_SECONDS)

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
    app = DSSChaliceApp(app_name=flask_app.name)
    app.log.setLevel(logging.DEBUG)

    @time_limited(app)
    def dispatch(*args, **kwargs):
        uri_params = app.current_request.uri_params or {}
        path = app.current_request.context["resourcePath"].format(**uri_params)
        req_body = app.current_request.raw_body if app.current_request._body is not None else None
        app.log.info(
            "[dispatch] path: %s query_string: %s method: %s",
            path,
            app.current_request.query_params,
            app.current_request.method,
        )
        with flask_app.test_request_context(path=path,
                                            base_url="https://{}".format(app.current_request.headers["host"]),
                                            query_string=app.current_request.query_params,
                                            method=app.current_request.method,
                                            headers=list(app.current_request.headers.items()),
                                            data=req_body,
                                            environ_base=app.current_request.stage_vars):
            flask_res = flask_app.full_dispatch_request()
        res_headers = dict(flask_res.headers)
        # API Gateway/Cloudfront adds a duplicate Content-Length with a different value (not sure why)
        res_headers.pop("Content-Length", None)
        return chalice.Response(status_code=flask_res._status_code,
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

    @app.route("/internal/logs/{group}", methods=["GET"])
    @time_limited(app)
    def get_logs(group):
        assert group in {"dss-dev", "dss-index-dev", "dss-sync-dev"}
        logs = []
        start_time = datetime.datetime.now() - datetime.timedelta(minutes=10)
        filter_args = dict(logGroupName="/aws/lambda/{}".format(group), startTime=int(start_time.timestamp()))
        if app.current_request.query_params and "pattern" in app.current_request.query_params:
            filter_args.update(filterPattern=app.current_request.query_params["pattern"])
        for event in paginate(boto3.client("logs").get_paginator("filter_log_events"), **filter_args):
            if "timestamp" not in event or "message" not in event:
                continue
            logs.append(event)
        return dict(logs=logs)

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

app = get_chalice_app(create_app().app)

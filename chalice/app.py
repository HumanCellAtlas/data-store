import os, sys, re, logging, collections, datetime

import flask
import chalice
import boto3

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib')) # noqa
sys.path.insert(0, pkg_root) # noqa

from dss import Config, create_app
from dss.events.handlers.sync import sync_blob
from dss.util import paginate


Config.set_config_by_env()


def get_chalice_app(flask_app):
    app = chalice.Chalice(app_name=flask_app.name)
    flask_app.debug = True
    app.debug = flask_app.debug
    app.log.setLevel(logging.DEBUG)

    def dispatch(*args, **kwargs):
        uri_params = app.current_request.uri_params or {}
        path = app.current_request.context["resourcePath"].format(**uri_params)
        req_body = app.current_request.raw_body if app.current_request._body is not None else None
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
    def serve_swagger_ui():
        return chalice.Response(status_code=200,
                                headers={"Content-Type": "text/html"},
                                body=swagger_ui_html)

    @app.route("/internal/notify", methods=["POST"])
    def handle_notification():
        event = app.current_request.json_body
        if event["kind"] == "storage#object" and event["selfLink"].startswith("https://www.googleapis.com/storage"):
            gs_key_name = event["name"]
            sync_result = sync_blob(source_platform="gs",
                                    source_key=gs_key_name,
                                    dest_platform="s3",
                                    logger=app.logger)
            return sync_result
        else:
            raise NotImplementedError()

    @app.route("/internal/logs/{group}", methods=["GET"])
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

    return app

app = get_chalice_app(create_app().app)

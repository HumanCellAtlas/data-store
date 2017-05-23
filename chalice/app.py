import os, sys, re, logging, collections

logging.basicConfig(level=logging.INFO)

import flask
from chalice import Chalice, Response

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))
assert os.path.exists(pkg_root)
sys.path.insert(0, pkg_root)

from dss import create_app


def get_chalice_app(flask_app):
    app = Chalice(app_name=__name__)
    app.debug = True

    def dispatch(*args, **kwargs):
        uri_params = app.current_request.uri_params or {}
        path = app.current_request.context["resourcePath"].format(**uri_params)
        with flask_app.test_request_context(path=path,
                                            base_url="https://{}".format(app.current_request.headers["host"]),
                                            query_string=app.current_request.query_params,
                                            method=app.current_request.method,
                                            content_type=None,
                                            content_length=None,
                                            errors_stream=None,
                                            headers=list(app.current_request.headers.items()),
                                            data=app.current_request.raw_body,
                                            environ_base=app.current_request.stage_vars):
            flask_res = flask_app.full_dispatch_request()
        return Response(status_code=flask_res._status_code,
                        headers=dict(flask_res.headers),
                        body="".join([c.decode() if isinstance(c, bytes) else c for c in flask_res.response]))

    routes = collections.defaultdict(list)
    for rule in flask_app.url_map.iter_rules():
        routes[re.sub(r"<(.+?)(:.+?)?>", r"{\1}", rule.rule).rstrip("/")] += rule.methods
    for route, methods in routes.items():
        print("ROUTE", route)
        app.route(route, methods=methods)(dispatch)
    return app

app = get_chalice_app(create_app().app)

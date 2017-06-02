import os, sys, re, logging, collections

import flask, chalice

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))
sys.path.insert(0, pkg_root)

from dss import create_app # noqa

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
        app.route(route, methods=methods)(dispatch)
    return app

app = get_chalice_app(create_app().app)

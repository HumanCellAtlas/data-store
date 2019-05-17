import collections
import json
import os
import re
import typing

import chalice
import requests

from connexion import FlaskApp
from connexion.resolver import RestyResolver

from fusillade import Config
from fusillade import logging


class ChaliceWithConnexion(chalice.Chalice):
    """
    Subclasses Chalice to host a Connexion app, route and proxy requests to it.
    """

    def __init__(self, swagger_spec_path, swagger_internal_spec_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.swagger_spec_path = swagger_spec_path
        self.swagger_internal_spec_path = swagger_internal_spec_path
        self.connexion_apis = []
        self.connexion_app = self.create_connexion_app()
        self.connexion_full_dispatch_request = self.connexion_app.app.full_dispatch_request
        self.connexion_request_context = self.connexion_app.app.test_request_context
        self.trailing_slash_routes = []
        routes = collections.defaultdict(list)
        for rule in self.connexion_app.app.url_map.iter_rules():
            route = re.sub(r"<(.+?)(:.+?)?>", r"{\1}", rule.rule)
            stripped_route = route.rstrip("/")
            if route.endswith("/"):
                self.trailing_slash_routes.append(stripped_route)
            route = routes.get(stripped_route, dict(methods=set(), content_types=[]))
            route['methods'] |= rule.methods
            route['content_types'].extend(self._get_content_types(rule))
            routes[stripped_route] = route
        for route, args in routes.items():
            self.route(route,
                       methods=list(set(args['methods']) - {"OPTIONS"}),
                       cors=True,
                       content_types=args['content_types'])(self.dispatch)

    def _get_content_types(self, rule) -> typing.List[str]:
        content_types = []
        methods = {}
        for api in self.connexion_apis:
            try:
                methods = api.specification.raw['paths'][rule.rule]
            except KeyError:
                continue
            else:
                # There shouldn't be duplicates across the APIs so take the first hit.
                break
        for method in methods.values():
            try:
                content_types = [content for content in method["requestBody"]['content'].keys()]
            except KeyError:
                return content_types
        return content_types

    def create_connexion_app(self):
        app = FlaskApp('fusillade')
        # The Flask/Connection app's logger has its own multi-line formatter and configuration. Rather than suppressing
        # it we let it do its thing, give it a special name and only enable it if Fusillade_DEBUG > 1.
        # Most of the Fusillade web app's logging is done through the FusilladeChaliceApp.app logger not the Flask
        # app's logger.
        app.app.logger_name = 'fus.api'
        debug = Config.debug_level() > 0
        app.app.debug = debug
        app.app.logger.info('Flask debug is %s.', 'enabled' if debug else 'disabled')

        resolver = RestyResolver("fusillade.api", collection_endpoint_name="list")
        self.connexion_apis.append(app.add_api(self.swagger_spec_path,
                                               resolver=resolver,
                                               validate_responses=True,
                                               arguments=os.environ,
                                               options={"swagger_path": self.swagger_spec_path}))
        self.connexion_apis.append(app.add_api(self.swagger_internal_spec_path, validate_responses=True))
        return app

    def dispatch(self, *args, **kwargs):
        """
        This is the main entry point into the connexion application.

        :param args:
        :param kwargs:
        :return:
        """
        cr = self.current_request
        context = cr.context
        uri_params = cr.uri_params or {}
        method = cr.method
        query_params = cr.query_params
        path = context["resourcePath"].format(**uri_params)
        if context["resourcePath"] in self.trailing_slash_routes:
            if context["path"].endswith("/"):
                path += "/"
            else:
                return chalice.Response(status_code=requests.codes.found, headers={"Location": path + "/"}, body="")
        req_body = cr.raw_body if cr._body is not None else None
        # TODO figure out of host should be os.environ["API_DOMAIN_NAME"]

        self.log.info(
            {
                "request": {
                    'method': method,
                    'path': path,
                    'sourceIp': context['identity']['sourceIp'],
                    'content-length': cr.headers.get('content-length', '-'),
                    'user-agent': cr.headers.get('user-agent'),
                    'query-params': str(query_params) if query_params is not None else ''
                }
            }
        )
        with self.connexion_request_context(path=path,
                                            base_url=os.environ["API_DOMAIN_NAME"],
                                            query_string=cr.query_params,
                                            method=method,
                                            headers=list(cr.headers.items()),
                                            data=req_body,
                                            environ_base=cr.stage_vars):
            try:
                flask_res = self.connexion_full_dispatch_request()
                status_code = flask_res._status_code
            except Exception as ex:
                self.log.exception(json.dumps(dict(
                    msg='The request failed!',
                    exception=ex
                )))
            finally:
                self.log.info(dict(
                    dispatch=dict(
                        method=method,
                        path=path,
                        status_code=status_code,
                        query_params=' ' + str(query_params) if query_params is not None else ''
                    )))
        res_headers = dict(flask_res.headers)
        res_headers.update(
            {"X-AWS-REQUEST-ID": self.lambda_context.aws_request_id,
             "Strict-Transport-Security": "max-age=31536000; includeSubDomains; preload"})
        res_headers.pop("Content-Length", None)
        return chalice.Response(status_code=status_code,
                                headers=res_headers,
                                body=b"".join([c for c in flask_res.response]).decode())


class ChaliceWithLoggingConfig(chalice.Chalice):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, configure_logs=False, **kwargs)
        self.debug = logging.is_debug()


class FusilladeServer(ChaliceWithConnexion, ChaliceWithLoggingConfig):
    pass

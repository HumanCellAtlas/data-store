#!/usr/bin/env python

"""
DSS description FIXME: elaborate
"""
import functools
import json
import logging
import os
import traceback

import bouncer
import requests
import connexion.apis.abstract
from connexion.apis.flask_api import FlaskApi
from connexion.decorators.validation import ParameterValidator, RequestBodyValidator
from connexion.lifecycle import ConnexionResponse
from connexion.operation import Operation
from connexion.resolver import RestyResolver
from connexion.exceptions import OAuthProblem, OAuthResponseProblem, OAuthScopeProblem
from werkzeug.exceptions import Forbidden

from dss.config import BucketConfig, Config, DeploymentStage, ESIndexType, ESDocType, Replica
from dss.error import DSSBindingException, DSSException, dss_handler

logger = logging.getLogger(__name__)


class DSSApp(connexion.App):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def common_error_handler(exception):
        """
        Generally, each route handler should be decorated with @dss_handler, which manages exceptions.  The two cases
        that fails are:

        1. handlers that are not decorated.
        2. handlers that return a code that is not in the swagger spec.

        In both cases, the exception would be punted here, and we return this very generic error that also happens to
        bypass all validation.
        """
        problem = {
            'status': requests.codes.server_error,
            'code': "unhandled_exception",
            'title': str(exception),
            'stacktrace': traceback.format_exc(),
        }
        if isinstance(exception, (OAuthProblem, OAuthResponseProblem, OAuthScopeProblem, Forbidden)):
            problem['status'] = exception.code
            problem['code'] = exception.__class__.__name__
            problem['title'] = exception.description
        return FlaskApi.get_response(ConnexionResponse(
            status_code=problem['status'],
            mimetype="application/problem+json",
            content_type="application/problem+json",
            body=problem,
        ))


class OperationWithAuthorizer(Operation):
    # Can't set authorized_domains here b/c config needs to be set before you
    # call get_allowed_email_domains or else when this file imported from
    # test_subscriptions, this will throw an error. Instead, testing_403 is
    # set to true in the test environment when needed.
    # TODO: Remove flag trigger
    testing_403 = False

    def oauth2_authorize(self, function):
        bouncer_ = bouncer.Bouncer(Config.get_email_whitelist_name())

        def wrapper(request):
            if "token_info" in request.context.values:
                token_info = request.context.values["token_info"]

                if not int(token_info["expires_in"]) > 0:
                    raise OAuthProblem(description="Authorization token has expired")
                if json.loads(token_info["email_verified"]) is not True:
                    raise OAuthProblem(description="User's email is not verified")
                if self.testing_403 or not bouncer_.is_authorized(token_info['email']):
                    raise Forbidden(description="User is not authorized to access this resource")
            return function(request)
        return wrapper

    def security_decorator(self, function):
        return super().security_decorator(self.oauth2_authorize(function))

connexion.apis.abstract.Operation = OperationWithAuthorizer


class DSSParameterValidator(ParameterValidator):
    """
    The ParameterValidator provided by Connexion immediately returns a value if the validation fails.  Therefore, our
    code is never invoked, and the common_error_handler in the connexion.App object is never called.  This means error
    messsages are not returned using our standard error formats.

    The solution is to trap the validation results, and if it fails, exit the validation flow.  We catch the exception
    at the top level where the various validators are called, and return a value according to our specs.
    """

    @staticmethod
    def validate_parameter(*args, **kwargs):
        result = ParameterValidator.validate_parameter(*args, **kwargs)
        if result is not None:
            raise DSSBindingException(result)
        return result

    def __call__(self, function):
        origwrapper = super().__call__(function)

        @functools.wraps(origwrapper)
        def wrapper(request):
            try:
                return origwrapper(request)
            except DSSBindingException as ex:
                status = ex.status
                code = ex.code
                title = ex.message
                stacktrace = traceback.format_exc()

                return FlaskApi.get_response(ConnexionResponse(
                    status_code=status,
                    mimetype="application/problem+json",
                    content_type="application/problem+json",
                    body={
                        'status': status,
                        'code': code,
                        'title': title,
                        'stacktrace': stacktrace,
                    },
                ))

        return wrapper


class DSSRequestBodyValidator(RequestBodyValidator):
    """
    The RequestBodyValidator provided by Connexion immediately returns a value if the validation fails.  Therefore, our
    code is never invoked, and the common_error_handler in the connexion.App object is never called.  This means error
    messsages are not returned using our standard error formats.

    The solution is to trap the validation results, and if it fails, exit the validation flow.  We catch the exception
    at the top level where the various validators are called, and return a value according to our specs.
    """

    def validate_schema(self, *args, **kwargs):
        result = super().validate_schema(*args, **kwargs)
        if result is not None:
            raise DSSBindingException(result.body['detail'])
        return result

    def __call__(self, function):
        origwrapper = super().__call__(function)

        @functools.wraps(origwrapper)
        def wrapper(request):
            try:
                return origwrapper(request)
            except DSSBindingException as ex:
                status = ex.status
                code = ex.code
                title = ex.message
                stacktrace = traceback.format_exc()

                return FlaskApi.get_response(ConnexionResponse(
                    status_code=status,
                    mimetype="application/problem+json",
                    content_type="application/problem+json",
                    body={
                        'status': status,
                        'code': code,
                        'title': title,
                        'stacktrace': stacktrace,
                    },
                ))

        return wrapper


def create_app():
    app = DSSApp(
        __name__,
        validator_map={
            'body': DSSRequestBodyValidator,
            'parameter': DSSParameterValidator,
        },
    )
    # The Flask/Connection app's logger has its own multi-line formatter and configuration. Rather than suppressing
    # it we let it do its thing, give it a special name and only enable it if DSS_DEBUG > 1. Most of the DSS web
    # app's logging is done through the DSSChaliceApp.app logger not the Flask app's logger.
    #
    app.app.logger_name = 'dss.api'
    debug = Config.debug_level() > 0
    app.app.debug = debug
    app.app.logger.info('Flask debug is %s.', 'enabled' if debug else 'disabled')

    resolver = RestyResolver("dss.api", collection_endpoint_name="list")
    app.add_api('../dss-api.yml', resolver=resolver, validate_responses=True, arguments=os.environ)
    return app

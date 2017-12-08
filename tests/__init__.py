import datetime
import functools
import time

from dss.util.version import datetime_to_version_format
import time
import uuid
import json
import io
import os
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account
from google.auth.credentials import with_scopes_if_required


def get_bundle_fqid():
    return f"{uuid.uuid4()}.{get_version()}"


def get_version():
    return datetime_to_version_format(datetime.datetime.utcnow())


def eventually(timeout: float, interval: float, errors: set={AssertionError}):
    """
    @eventually runs a test until all assertions are satisfied or a timeout is reached.
    :param timeout: time until the test fails
    :param interval: time between attempts of the test
    :param errors: the exceptions to catch and retry on
    :return: the result of the function or a raised assertion error
    """
    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
            timeout_time = time.time() + timeout
            error_tuple = tuple(errors)
            while True:
                try:
                    return func(*args, **kwargs)
                except error_tuple:
                    if time.time() >= timeout_time:
                        raise
                    time.sleep(interval)

        return call

    return decorate


def get_auth_header(real_header=True, filepath=None):
    credential_file = filepath if filepath else os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    with io.open(credential_file) as fh:
        info = json.load(fh)
        credentials = service_account.Credentials.from_service_account_info(info)
    credentials = with_scopes_if_required(credentials, scopes=["https://www.googleapis.com/auth/userinfo.email"])

    r = google.auth.transport.requests.Request()
    credentials.refresh(r)
    r.session.close()

    token = credentials.token if real_header else str(uuid.uuid4())

    return {"Authorization": f"Bearer {token}"}

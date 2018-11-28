import inspect
import os
import uuid
import time
from collections import defaultdict

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))  # noqa

from dss.util.types import LambdaContext
from .assert_mixin import DSSAssertResponse, DSSAssertMixin, ExpectedErrorFields
from .storage_mixin import DSSStorageMixin, TestBundle
from .testmode import integration, standalone
from .upload_mixin import DSSUploadMixin
from .auth_tests_mixin import TestAuthMixin


def get_env(varname):
    if varname not in os.environ:
        raise RuntimeError(
            "Please set the {} environment variable".format(varname))
    return os.environ[varname]


def generate_test_key() -> str:
    callerframerecord = inspect.stack()[1]  # 0 represents this line, 1 represents line at caller.
    frame = callerframerecord[0]
    info = inspect.getframeinfo(frame)
    filename = os.path.basename(info.filename)
    unique_key = str(uuid.uuid4())

    return f"{filename}/{info.function}/{unique_key}"


def determine_auth_configuration_from_swagger():
    path_section = False  # bool flag to notify if we're in the section containing the API call definitions
    call_section = None  # an api endpoint, e.g.: /subscription, /file/{uuid}, etc.
    request_section = None  # a request call, e.g.: get, put, delete, etc.
    security_endpoints = defaultdict(list)
    with open(os.path.join(pkg_root, 'dss-api.yml'), 'r') as f:
        for line in f:
            # If not indented at all, we're in a new section, so reset.
            if not line.startswith(' ') and path_section and line.strip() != '':
                path_section = False

            # Check if we're in the paths section.
            if line.startswith('paths:'):
                path_section = True
            # Check if we're in an api path section.
            elif line.startswith('  /') and line.strip().endswith(':'):
                call_section = line.strip()[:-1]
            elif line.startswith('      security:'):
                security_endpoints[call_section].append(request_section)
            # If properly indented and we're in the correct section, this will be a call request.
            elif line.startswith('    ') and not line.startswith('     ') and \
                    path_section and line.strip().endswith(':'):
                request_section = line.strip()[:-1]
    return security_endpoints


# noinspection PyAbstractClass
class MockLambdaContext(LambdaContext):
    """
    A mock of the class an instance of which the AWS Lambda Python runtime injects into each invocation.
    """

    def __init__(self, timeout: float = 300.0) -> None:
        self.deadline = time.time() + timeout

    def get_remaining_time_in_millis(self):
        return int(max(0.0, self.deadline - time.time()) * 1000)

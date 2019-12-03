import inspect
import os
import uuid
import time

from dss.util.types import LambdaContext
from .assert_mixin import DSSAssertResponse, DSSAssertMixin, ExpectedErrorFields
from .storage_mixin import DSSStorageMixin, TestBundle
from .testmode import integration, standalone
from .upload_mixin import DSSUploadMixin
from .auth_tests_mixin import TestAuthMixin
from .mock_storage_handler import MockStorageHandler


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


# noinspection PyAbstractClass
class MockLambdaContext(LambdaContext):
    """
    A mock of the class an instance of which the AWS Lambda Python runtime injects into each invocation.
    """

    def __init__(self, timeout: float = 300.0) -> None:
        self.deadline = time.time() + timeout

    def get_remaining_time_in_millis(self):
        return int(max(0.0, self.deadline - time.time()) * 1000)

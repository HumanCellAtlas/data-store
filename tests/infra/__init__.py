import inspect
import os
import uuid

from .assert_mixin import DSSAssertResponse, DSSAssertMixin, ExpectedErrorFields
from .storage_mixin import DSSStorageMixin, TestBundle
from .testmode import integration, standalone
from .upload_mixin import DSSUploadMixin


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

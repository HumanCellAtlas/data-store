import os
from functools import wraps
import typing
from aws_xray_sdk.core import xray_recorder, patch
from aws_xray_sdk.core.context import Context


DSS_XRAY_TRACE = int(os.environ.get('DSS_XRAY_TRACE', '0')) > 0  # noqa

patched = False

if DSS_XRAY_TRACE and not patched:  # noqa
    patch(('boto3', 'requests'))
    xray_recorder.configure(context_missing='LOG_ERROR')
    patched = True


def capture_segment(name: str) -> typing.Callable:
    """Used to conditionally wrap functions with `xray_recorder.capture` when DSS_XRAY_TRACE is enabled."""
    def decorate(func):
        if DSS_XRAY_TRACE:  # noqa
            from aws_xray_sdk.core import xray_recorder

            @wraps(func)
            def call(*args, **kwargs):
                return xray_recorder.capture(name)(func)(*args, **kwargs)
        else:

            @wraps(func)
            def call(*args, **kwargs):
                return func(*args, **kwargs)
        return call
    return decorate


def begin_segment(name: str) -> None:
    if DSS_XRAY_TRACE:
        xray_recorder.begin_subsegment(name)


def end_segment(name: str) -> None:
    if DSS_XRAY_TRACE:
        xray_recorder.end_subsegment(name)

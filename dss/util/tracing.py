import os
import time
from functools import wraps
import typing
from aws_xray_sdk.core import xray_recorder, patch
import logging

logger = logging.getLogger(__name__)
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


def begin_segment(name: typing.Optional[str]) -> None:
    if DSS_XRAY_TRACE:
        logger.debug(f"Begin subsegment {name}")
        xray_recorder.begin_subsegment(name)

def end_segment(name: typing.Optional[str]) -> None:
    if DSS_XRAY_TRACE:
        logger.debug(f"End subsegment {name}")
        end_time = time.time()
        xray_recorder.end_subsegment(end_time)


class Subsegment:
    def __init__(self, name: typing.Optional[str]) -> None:
        self.name = name

    def __enter__(self) -> None:
        begin_segment(self.name)

    def __exit__(self) -> None:
        end_segment(self.name)

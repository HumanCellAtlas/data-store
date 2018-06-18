import os
import time

from functools import wraps
import typing
from aws_xray_sdk.core.exceptions.exceptions import SegmentNotFoundException
from aws_xray_sdk.core import xray_recorder, patch
from aws_xray_sdk.core.models.subsegment import Subsegment as xray_Subsegment
import logging

logger = logging.getLogger(__name__)
DSS_XRAY_TRACE = int(os.environ.get('DSS_XRAY_TRACE', '0')) > 0  # noqa

patched = False

if DSS_XRAY_TRACE and not patched:  # noqa
    patch(('boto3', 'requests'))
    xray_recorder.configure(context_missing='LOG_ERROR')
    patched = True


class XrayLoggerFilter(logging.Filter):
    def filter(self, record):

        # get aws generated xray tracing header from env
        # https://docs.aws.amazon.com/lambda/latest/dg/lambda-x-ray.html
        header = os.getenv('_X_AMZN_TRACE_ID', '')

        # parse xray-tracing header
        # https://docs.aws.amazon.com/xray/latest/devguide/xray-concepts.html#xray-concepts-tracingheader
        if header is not '':
            header = {key: item for key, item in [i.split('=') for i in header.split(';')]}
        record.xray_trace_header = header
        return True

def configure_xray_logging(handler) -> None:
    if DSS_XRAY_TRACE:
        handler.addFilter(XrayLoggerFilter())
        handler.formatter.add_required_fields(['xray_trace_header'])


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


def begin_subsegment(name: typing.Optional[str]) -> typing.Optional[xray_Subsegment]:
    if DSS_XRAY_TRACE:
        logger.debug(f"Begin subsegment {name}")
        return xray_recorder.begin_subsegment(name)
    return None

def end_subsegment(name: typing.Optional[str], end_time: float) -> None:
    if DSS_XRAY_TRACE:
        logger.debug(f"End subsegment {name}")
        xray_recorder.end_subsegment(end_time)


class Subsegment:
    def __init__(self, name: typing.Optional[str]) -> None:
        self.name = name

    def __enter__(self) -> xray_Subsegment:
        self._subsegment = begin_subsegment(self.name)
        return self._subsegment

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        rv = True
        end_time = time.time()
        if exc_type:
            rv = False
            if DSS_XRAY_TRACE:
                if self._subsegment is not None:
                    self._subsegment.add_exception(exc_val, exc_tb)
                else:
                    raise SegmentNotFoundException("Subsegment context manager is missing subsegment!")
        end_subsegment(self.name, end_time)
        return rv

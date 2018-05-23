import os
from aws_xray_sdk.core import xray_recorder, patch
from aws_xray_sdk.core.context import Context

DSS_XRAY_TRACE = int(os.environ.get('XRAY_TRACE', '0')) > 0

if DSS_XRAY_TRACE:
    patch(('boto3', 'requests'))
    xray_recorder.configure(
        service='DSS',
        dynamic_naming=f"*{os.environ['API_DOMAIN_NAME']}*",
        context=Context(),
        context_missing='LOG_ERROR'
    )


def begin_segment(name):
    if DSS_XRAY_TRACE:
        xray_recorder.begin_subsegment(name)


def end_segment(name):
    if DSS_XRAY_TRACE:
        xray_recorder.end_subsegment(name)


import os
import sys
import string
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.stepfunctions.visitation.sfn_definitions import sentinel_sfn
from dss.stepfunctions.visitation import StatusCode, Sentinel
from dss.stepfunctions.visitation.utils import *


logger = dss.get_logger()


app = domovoi.Domovoi()


@app.step_function_task(
    state_name = 'Initialize',
    state_machine_definition = sentinel_sfn
)
def initialize(event, context):

    sentinel = Sentinel(
        ** event,
        logger = logger
    )

    validate_bucket(
        sentinel.bucket
    )

    alphanumeric = string.ascii_lowercase[:6] + '0987654321'

    sentinel.waiting = [f'{a}{b}' for a in alphanumeric for b in alphanumeric]

    sentinel.code = StatusCode.RUNNING.name

    return sentinel.to_dict()


@app.step_function_task(
    state_name = 'MusterWalkers',
    state_machine_definition = sentinel_sfn
)
def muster_walkers(event, context):

    sentinel = Sentinel(
        ** event,
        logger = logger
    )

    sentinel.muster()

    return sentinel.to_dict()


@app.step_function_task(
    state_name = 'Succeeded',
    state_machine_definition = sentinel_sfn
)
def succeeded(event, context):
    pass


@app.step_function_task(
    state_name = 'NotifyFailure',
    state_machine_definition = sentinel_sfn
)
def notify_failure(event, context):
    pass

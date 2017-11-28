
import os
import sys
import string
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.stepfunctions import visitation
from dss.stepfunctions.visitation.sfn_definitions import sentinel_sfn
from dss.stepfunctions.visitation import StatusCode
from dss.stepfunctions.visitation.utils import *
from dss.stepfunctions.visitation.registered_visitations import registered_visitations


logger = dss.get_logger()


app = domovoi.Domovoi()


def vis_obj(event):

    class_name = event['visitation_class_name']

    vis_class = registered_visitations[class_name]

    return vis_class.with_sentinel_state(
        event,
        logger
    )


@app.step_function_task(
    state_name = 'Initialize',
    state_machine_definition = sentinel_sfn
)
def initialize(event, context):

    sentinel = vis_obj(
        event
    )

    sentinel.initialize()

    sentinel.code = StatusCode.RUNNING.name

    return sentinel.propagate_state()


@app.step_function_task(
    state_name = 'MusterWalkers',
    state_machine_definition = sentinel_sfn
)
def muster_walkers(event, context):

    sentinel = vis_obj(
        event
    )

    sentinel.muster()

    return sentinel.propagate_state()


@app.step_function_task(
    state_name = 'Succeeded',
    state_machine_definition = sentinel_sfn
)
def succeeded(event, context):

    sentinel = vis_obj(
        event
    )

    sentinel.finalize()

    return sentinel.propagate_state()


@app.step_function_task(
    state_name = 'Failed',
    state_machine_definition = sentinel_sfn
)
def failed(event, context):

    sentinel = vis_obj(
        event
    )

    sentinel.finalize_failed()

    return sentinel.propagate_state()

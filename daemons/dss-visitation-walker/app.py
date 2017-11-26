
import os
import sys
import json
import boto3
import domovoi
from uuid import uuid4

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.stepfunctions import visitation
from dss.stepfunctions.visitation.sfn_definitions import walker_sfn
from dss.stepfunctions.visitation import StatusCode, DSSVisitationExceptionSkipItem
from dss.stepfunctions.visitation.utils import *


logger = dss.get_logger()


app = domovoi.Domovoi()


def vis_obj(event):

    class_name = event['visitation_class_name']

    vis_class = visitation.registered_visitations[class_name]

    return vis_class.walker_state(
        event
    )


@app.step_function_task(
    state_name = 'Initialize',
    state_machine_definition = walker_sfn
)
def initialize(event, context):

    walker = vis_obj(
        event
    )

    walker.initialize_walker()

    walker.code = StatusCode.RUNNING.name

    return walker.propagate_state()


@app.step_function_task(
    state_name = 'Walk',
    state_machine_definition = walker_sfn
)
def walk(event, context):

    # TODO: use the lambdaexecutor for timed work
    
    walker = vis_obj(
        event
    )

    walker.walk()

    return walker.propagate_state()


@app.step_function_task(
    state_name = "Succeeded",
    state_machine_definition = walker_sfn
)
def succeeded(event, context):

    walker = vis_obj(
        event
    )

    walker.finalize_walker()

    return walker.propagate_state()


@app.step_function_task(
    state_name = "Failed",
    state_machine_definition = walker_sfn
)
def failed(event, context):

    walker = vis_obj(
        event
    )

    walker.finalize_failed_walker()

    return walker.propagate_state()

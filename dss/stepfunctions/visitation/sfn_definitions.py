
from ... import get_logger
from . import StatusCode, DSSVisitationException
from .registered_visitations import registered_visitations


WORK_TIMEOUT = 250


logger = get_logger()


def vis_obj(event, sfn):
    class_name = event.get('visitation_class_name', None)

    if class_name is None:
        raise DSSVisitationException('Input to visitation sentinel should include a registered vistation class')

    vis_class = registered_visitations.get(class_name, None)

    if vis_class is None:
        raise DSSVisitationException('Unknown visitation class')

    if 'walker' == sfn:
        return vis_class.with_walker_state(event, logger)
    elif 'sentinel' == sfn:
        return vis_class.with_sentinel_state(event, logger)
    else:
        raise DSSVisitationException('Unknown stepfunction', sfn)


def initialize(event, context):
    if event.get('is_sentinel', False):
        inst = vis_obj(event, 'sentinel')
        inst.sentinel_initialize()
        inst.code = StatusCode.RUNNING.name
    else:
        inst = vis_obj(event, 'walker')
        inst.walker_initialize()
        inst.code = StatusCode.RUNNING.name

    return inst.get_state()


def work(event, context):
    if event.get('is_sentinel', False):
        inst = vis_obj(event, 'sentinel')
        inst.sentinel_muster()
    else:
        inst = vis_obj(event, 'walker')
        inst.walker_walk(WORK_TIMEOUT)
        inst.wait_time = 0.1

    return inst.get_state()


def finalize(event, context):
    if event.get('is_sentinel', False):
        inst = vis_obj(event, 'sentinel')
        inst.sentinel_finalize()
    else:
        inst = vis_obj(event, 'walker')
        inst.walker_finalize()

    return inst.get_state()


def finalize_failed(event, context):
    if event.get('is_sentinel', False):
        inst = vis_obj(event, 'sentinel')
        inst.sentinel_finalize_failed()
    else:
        inst = vis_obj(event, 'walker')
        inst.walker_finalize_failed()

    return inst.get_state()


def _retry(interval_seconds, max_attempts=3, backoff_rate=2):
    return [
        {
            "ErrorEquals": ["DSSVisitationExceptionRetry"],
            "IntervalSeconds": interval_seconds,
            "MaxAttempts": 20,
            "BackoffRate": 1
        },
        {
            "ErrorEquals": ["States.Timeout"],
            "IntervalSeconds": interval_seconds,
            "MaxAttempts": max_attempts,
            "BackoffRate": backoff_rate
        }
    ]


def _catch_to_state(next_state):
    return [{
        "ErrorEquals": [
            "States.ALL"
        ],
        "Next": next_state
    }]


sfn = {
    "Comment": "Visitation State Machine",
    "StartAt": "Initialize",
    "States": {
        "Initialize": {
            "Type": "Task",
            "Resource": initialize,
            "Catch": _catch_to_state("Failed"),
            "Next": "CheckStatus"
        },
        "Walk": {
            "Type": "Task",
            "Resource": work,
            "Retry": _retry(interval_seconds=5),
            "Catch": _catch_to_state("Failed"),
            "TimeoutSeconds": 295,
            "Next": "CheckStatus"
        },
        "CheckStatus": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.code",
                    "StringEquals": "RUNNING",
                    "Next": "Wait"
                }
            ],
            "Default": "Finalize"
        },
        "Wait": {
            "Type": "Wait",
            "SecondsPath": "$.wait_time",
            "Next": "Walk"
        },
        "Failed": {
            "Type": "Task",
            "Resource": finalize_failed,
            "Catch": _catch_to_state("Fail"),
            "Next": "Fail"
        },
        "Fail": {
            "Type": "Fail",
        },
        "Finalize": {
            "Type": "Task",
            "Resource": finalize,
            "Catch": _catch_to_state("Failed"),
            "End": True
        }
    }
}

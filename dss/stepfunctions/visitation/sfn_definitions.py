
from ... import get_logger
from . import StatusCode, DSSVisitationException
from .registered_visitations import registered_visitations


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


def walker_initialize(event, context):
    walker = vis_obj(event, 'walker')
    walker.walker_initialize()
    walker.code = StatusCode.RUNNING.name
    return walker.get_state()


def walker_walk(event, context):
    walker = vis_obj(event, 'walker')
    walker.walker_walk()
    return walker.get_state()


def walker_finalize(event, context):
    walker = vis_obj(event, 'walker')
    walker.walker_finalize()
    return walker.get_state()


def walker_failed(event, context):
    walker = vis_obj(event, 'walker')
    walker.walker_finalize_failed()
    return walker.get_state()


def sentinel_initialize(event, context):
    sentinel = vis_obj(event, 'sentinel')
    sentinel.sentinel_initialize()
    sentinel.code = StatusCode.RUNNING.name
    return sentinel.get_state()


def sentinel_muster_walkers(event, context):
    sentinel = vis_obj(event, 'sentinel')
    sentinel.sentinel_muster()
    return sentinel.get_state()


def sentinel_finalize(event, context):
    sentinel = vis_obj(event, 'sentinel')
    sentinel.sentinel_finalize()
    return sentinel.get_state()


def sentinel_failed(event, context):
    sentinel = vis_obj(event, 'sentinel')
    sentinel.sentinel_finalize_failed()
    return sentinel.get_state()


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


sentinel_sfn = {
    "Comment": "DSS Re-index service state machine sentinel",
    "StartAt": "Initialize",
    "States": {
        "Initialize": {
            "Type": "Task",
            "Resource": sentinel_initialize,
            "Catch": _catch_to_state("Failed"),
            "Next": "MusterWalkers"
        },
        "MusterWalkers": {
            "Type": "Task",
            "Resource": sentinel_muster_walkers,
            "Retry": _retry(interval_seconds=60),
            "Catch": _catch_to_state("Failed"),
            "TimeoutSeconds": 240,
            "Next": "CheckStatus"
        },
        "CheckStatus": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.code",
                    "StringEquals": "RUNNING",
                    "Next": "Wait"
                },
            ],
            "Default": "Finalize"
        },
        "Wait": {
            "Type": "Wait",
            "SecondsPath": "$.wait_time",
            "Next": "MusterWalkers"
        },
        "Failed": {
            "Type": "Task",
            "Resource": sentinel_failed,
            "Catch": _catch_to_state("Fail"),
            "Next": "Fail"
        },
        "Fail": {
            "Type": "Fail",
        },
        "Finalize": {
            "Type": "Task",
            "Resource": sentinel_finalize,
            "Catch": _catch_to_state("Failed"),
            "End": True
        }
    }
}

walker_sfn = {
    "Comment": "prefix walker",
    "StartAt": "Initialize",
    "States": {
        "Initialize": {
            "Type": "Task",
            "Resource": walker_initialize,
            "Catch": _catch_to_state("Failed"),
            "Next": "CheckStatus"
        },
        "Walk": {
            "Type": "Task",
            "Resource": walker_walk,
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
                    "Next": "Walk"
                }
            ],
            "Default": "Finalize"
        },
        "Failed": {
            "Type": "Task",
            "Resource": walker_failed,
            "Catch": _catch_to_state("Fail"),
            "Next": "Fail"
        },
        "Fail": {
            "Type": "Fail",
        },
        "Finalize": {
            "Type": "Task",
            "Resource": walker_finalize,
            "Catch": _catch_to_state("Failed"),
            "End": True
        }
    }
}

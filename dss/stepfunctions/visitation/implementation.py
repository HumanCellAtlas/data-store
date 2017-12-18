
from ... import get_logger
from ...stepfunctions import generator
from . import Visitation, DSSVisitationException
from .registered_visitations import registered_visitations


logger = get_logger()


THREADPOOL_PARALLEL_FACTOR = 32


def vis_obj(event, sfn):
    class_name = event.get('_visitation_class_name', None)

    if class_name is None:
        raise DSSVisitationException('Input to visitation sentinel should include a registered vistation class')

    vis_class = registered_visitations.get(class_name, None)

    if vis_class is None:
        raise DSSVisitationException('Unknown visitation class')

    if 'walker' == sfn:
        return vis_class._with_walker_state(event, logger)
    elif 'sentinel' == sfn:
        return vis_class._with_sentinel_state(event, logger)
    else:
        raise DSSVisitationException('Unknown stepfunction', sfn)


def walker_initialize(event, context, branch_id):
    branch = branch_id[-1]
    sentinel = vis_obj(event, 'sentinel')
    walker = vis_obj(event, 'walker')

    walker.work_id = sentinel._processing_work_ids[branch]
    walker.walker_initialize()

    if branch == 0:
        """
        propagate the sentinel state through branch 0, avoiding state key collisions
        """
        walker._sentinel_state_copy = sentinel.get_state()

    return walker.get_state()


def walker_walk(event, context, branch_id):
    walker = vis_obj(event, 'walker')
    if walker.work_id is not None:
        walker.walker_walk()
    return walker.get_state()


def walker_finalize(event, context, branch_id):
    walker = vis_obj(event, 'walker')
    if walker.work_id is not None:
        walker.walker_finalize()
    return walker.get_state()


def walker_failed(event, context, branch_id):
    walker = vis_obj(event, 'walker')
    if walker.work_id is not None:
        walker.walker_finalize_failed()
    return walker.get_state()


def sentinel_initialize(event, context):
    sentinel = vis_obj(event, 'sentinel')

    sentinel.sentinel_initialize()

    if 0 == len(sentinel.work_ids):
        raise DSSVisitationException('Expected len(sentinel.work_ids)>0.')

    return sentinel.get_state()


def muster(event, context):
    sentinel = vis_obj(event, 'sentinel')

    k = sentinel._number_of_workers
    sentinel._processing_work_ids = sentinel.work_ids[:k]
    sentinel.work_ids = sentinel.work_ids[k:]

    sentinel._number_of_workers = min(
        sentinel._number_of_workers,
        len(sentinel._processing_work_ids)
    )

    return sentinel.get_state()


def sentinel_join(event, context):
    walkers = [vis_obj(e, 'walker') for e in event]

    """
    Pick up the sentinel state from branch 0 and clear it from the walker.
    """
    sentinel = vis_obj(walkers[0]._sentinel_state_copy, 'sentinel')
    walkers[0]._sentinel_state_copy = None

    if not sentinel.work_ids:
        sentinel.is_finished = True
    else:
        sentinel.is_finished = False

    return sentinel.get_state()


def sentinel_finalize(event, context):
    sentinel = vis_obj(event, 'sentinel')
    sentinel.sentinel_finalize()
    return sentinel.get_state()


def sentinel_failed(event, context):
    sentinel = vis_obj(event, 'sentinel')
    sentinel.sentinel_finalize_failed()
    return sentinel.get_state()


_retry = [
    {
        "ErrorEquals": ["DSSVisitationExceptionRetry"],
        "IntervalSeconds": 5,
        "MaxAttempts": 20,
        "BackoffRate": 1
    },
    {
        "ErrorEquals": ["States.Timeout"],
        "IntervalSeconds": 5,
        "MaxAttempts": 3,
        "BackoffRate": 1.5
    }
]


def _catch_to_state(next_state):
    return [{
        "ErrorEquals": [
            "States.ALL"
        ],
        "Next": next_state
    }]


walker_sfn = {
    "StartAt": "IsActive{t}",
    "States": {
        "IsActive{t}": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$._number_of_workers",
                    "NumericLessThanEquals": "int({t})",
                    "Next": "Inactive{t}"
                }
            ],
            "Default": "Initialize{t}"
        },
        "Initialize{t}": {
            "Type": "Task",
            "Resource": walker_initialize,
            "Retry": _retry,
            "Catch": _catch_to_state("Failed{t}"),
            "Next": "CheckStatus{t}"
        },
        "Walk{t}": {
            "Type": "Task",
            "Resource": walker_walk,
            "Retry": _retry,
            "Catch": _catch_to_state("Failed{t}"),
            "TimeoutSeconds": 295,
            "Next": "CheckStatus{t}"
        },
        "CheckStatus{t}": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.is_finished",
                    "BooleanEquals": False,
                    "Next": "Walk{t}"
                }
            ],
            "Default": "Finalize{t}"
        },
        "Failed{t}": {
            "Type": "Task",
            "Resource": walker_failed,
            "Retry": _retry,
            "Catch": _catch_to_state("Fail{t}"),
            "Next": "Fail{t}"
        },
        "Fail{t}": {
            "Type": "Fail",
        },
        "Finalize{t}": {
            "Type": "Task",
            "Resource": walker_finalize,
            "Retry": _retry,
            "Catch": _catch_to_state("Failed{t}"),
            "End": True
        },
        "Inactive{t}": {
            "Type": "Pass",
            "End": True
        }
    }
}


sfn = {
    "Comment": "DSS Re-index service state machine sentinel",
    "StartAt": "Initialize",
    "States": {
        "Initialize": {
            "Type": "Task",
            "Resource": sentinel_initialize,
            'Retry': _retry,
            "Catch": _catch_to_state("Failed"),
            "Next": "Muster"
        },
        'Muster': {
            'Type': 'Task',
            'Resource': muster,
            'Catch': _catch_to_state('Failed'),
            'TimeoutSeconds': 240,
            'Next': 'Threadpool'
        },
        "Threadpool": {
            "Type": "Parallel",
            "Branches": generator.ThreadPoolAnnotation(walker_sfn, THREADPOOL_PARALLEL_FACTOR, "{t}"),
            "Retry": _retry,
            "Next": "Join",
        },
        "Join": {
            'Type': 'Task',
            'Resource': sentinel_join,
            'Catch': _catch_to_state('Failed'),
            'Next': 'CheckStatus'
        },
        "CheckStatus": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.is_finished",
                    "BooleanEquals": False,
                    "Next": "Wait"
                },
            ],
            "Default": "Finalize"
        },
        "Wait": {
            "Type": "Wait",
            "SecondsPath": "$.wait_time",
            "Next": "Muster"
        },
        "Failed": {
            "Type": "Task",
            "Resource": sentinel_failed,
            'Retry': _retry,
            "Catch": _catch_to_state("Fail"),
            "Next": "Fail"
        },
        "Fail": {
            "Type": "Fail",
        },
        "Finalize": {
            "Type": "Task",
            "Resource": sentinel_finalize,
            'Retry': _retry,
            "Catch": _catch_to_state("Failed"),
            "End": True
        }
    }
}

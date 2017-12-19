
from ... import get_logger
from ...stepfunctions import generator
from .registered_visitations import registered_visitations
from . import Visitation, DSSVisitationException, WalkerStatus


logger = get_logger()


THREADPOOL_PARALLEL_FACTOR = 32


def vis_obj(event):
    class_name = event.get('_visitation_class_name', None)

    if class_name is None:
        raise DSSVisitationException('Input to visitation job should include a registered visitation class')

    vis_class = registered_visitations.get(class_name, None)

    if vis_class is None:
        raise DSSVisitationException('Unknown visitation class')

    return vis_class._with_state(event, logger)


def job_initialize(event, context):
    obj = vis_obj(event)
    obj.job_initialize()

    if obj._number_of_workers > len(obj.work_ids):
        raise DSSVisitationException('Expected len(.work_ids)>=number_of_workers.')

    work_assignments = [list() for _ in range(obj._number_of_workers)]
    for i, w in enumerate(obj.work_ids):
        work_assignments[i % obj._number_of_workers].append(w)

    obj.work_ids = work_assignments
    return obj.get_state()


def job_finalize(event, context):
    obj = vis_obj(event[0])
    obj.job_finalize()
    return obj.get_state()


def job_failed(event, context):
    job = vis_obj(event)
    job.job_finalize_failed()
    return job.get_state()


def walker_initialize(event, context, branch_id):
    branch = branch_id[-1]
    walker = vis_obj(event)

    if isinstance(walker.work_ids[0], list):
        walker.work_ids = walker.work_ids[branch]

    walker.work_id = walker.work_ids[0]
    walker.work_ids = walker.work_ids[1:]
    walker._status = WalkerStatus.walk.name

    for k, v in walker.walker_state_spec.items():
        """
        Re-initialize user space walker state.
        """
        setattr(walker, k, v() if callable(v) else v)

    walker.walker_initialize()

    return walker.get_state()


def walker_walk(event, context, branch_id):
    walker = vis_obj(event)
    walker.walker_walk()
    return walker.get_state()


def walker_finalize(event, context, branch_id):
    walker = vis_obj(event)
    walker.walker_finalize()

    if len(walker.work_ids):
        walker._status = WalkerStatus.init.name
    else:
        walker._status = WalkerStatus.end.name

    return walker.get_state()


def walker_failed(event, context, branch_id):
    walker = vis_obj(event)
    walker.walker_finalize_failed()
    return walker.get_state()


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
                    "Next": "End{t}"
                }
            ],
            "Default": "CheckStatus{t}"
        },
        "CheckStatus{t}": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$._status",
                    "StringEquals": WalkerStatus.init.name,
                    "Next": "Initialize{t}"
                },
                {
                    "Variable": "$._status",
                    "StringEquals": WalkerStatus.walk.name,
                    "Next": "Walk{t}"
                },
                {
                    "Variable": "$._status",
                    "StringEquals": WalkerStatus.finished.name,
                    "Next": "Finalize{t}"
                },
                {
                    "Variable": "$._status",
                    "StringEquals": WalkerStatus.end.name,
                    "Next": "End{t}"
                }
            ],
            "Default": "Finalize{t}"
        },
        "Initialize{t}": {
            "Type": "Task",
            "Resource": walker_initialize,
            "Retry": _retry,
            "Catch": _catch_to_state("Failed{t}"),
            "Next": "Walk{t}"
        },
        "Walk{t}": {
            "Type": "Task",
            "Resource": walker_walk,
            "Retry": _retry,
            "Catch": _catch_to_state("Failed{t}"),
            "TimeoutSeconds": 295,
            "Next": "CheckStatus{t}"
        },
        "Finalize{t}": {
            "Type": "Task",
            "Resource": walker_finalize,
            "Retry": _retry,
            "Catch": _catch_to_state("Failed{t}"),
            "Next": "CheckStatus{t}"
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
        "End{t}": {
            "Type": "Pass",
            "End": True
        }
    }
}


sfn = {
    "Comment": "DSS Re-index service state machine job",
    "StartAt": "Initialize",
    "States": {
        "Initialize": {
            "Type": "Task",
            "Resource": job_initialize,
            'Retry': _retry,
            "Catch": _catch_to_state("Failed"),
            "Next": "Threadpool"
        },
        "Threadpool": {
            "Type": "Parallel",
            "Branches": generator.ThreadPoolAnnotation(walker_sfn, THREADPOOL_PARALLEL_FACTOR, "{t}"),
            "Retry": _retry,
            "Next": "Finalize",
        },
        "Finalize": {
            'Type': 'Task',
            "Resource": job_finalize,
            'Catch': _catch_to_state('Failed'),
            "End": True
        },
        "Failed": {
            "Type": "Task",
            "Resource": job_failed,
            'Retry': _retry,
            "Catch": _catch_to_state("Fail"),
            "Next": "Fail"
        },
        "Fail": {
            "Type": "Fail",
        }
    }
}

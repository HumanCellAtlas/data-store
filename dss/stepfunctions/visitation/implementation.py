import copy
import logging

from dss.util.time import RemainingLambdaContextTime
from .registered_visitations import registered_visitations
from . import DSSVisitationException, WalkerStatus


logger = logging.getLogger(__name__)


THREADPOOL_PARALLEL_FACTOR = 32


def vis_obj(event, context):
    class_name = event.get('_visitation_class_name', None)

    if class_name is None:
        raise DSSVisitationException('Input to visitation job should include a registered visitation class')

    vis_class = registered_visitations.get(class_name, None)

    if vis_class is None:
        raise DSSVisitationException('Unknown visitation class')

    remaining_time = RemainingLambdaContextTime(context)
    return vis_class._with_state(event, remaining_time)


def job_initialize(event, context):
    obj = vis_obj(event, context)
    obj.job_initialize()

    if obj._number_of_workers > len(obj.work_ids):
        raise DSSVisitationException('Expected len(.work_ids)>=number_of_workers.')

    work_assignments = [list() for _ in range(obj._number_of_workers)]
    for i, w in enumerate(obj.work_ids):
        work_assignments[i % obj._number_of_workers].append(w)

    obj.work_ids = work_assignments
    return obj.get_state()


def job_finalize(event, context):
    obj = vis_obj(event, context)
    obj.job_finalize()
    return obj.get_state()


def job_failed(event, context):
    job = vis_obj(event, context)
    job.job_finalize_failed()
    return job.get_state()


def walker_initialize(event, context, branch):
    walker = vis_obj(event, context)

    if isinstance(walker.work_ids[0], list):
        walker.work_ids = walker.work_ids[branch]

    walker.work_id = walker.work_ids[0]
    walker.work_ids = walker.work_ids[1:]
    walker._status = WalkerStatus.walk.name

    for k, v in walker.walker_state_spec.items():
        """
        Re-initialize user space walker state.
        """
        setattr(walker, k, v() if callable(v) else copy.deepcopy(v))

    walker.walker_initialize()

    return walker.get_state()


def walker_walk(event, context, branch):
    walker = vis_obj(event, context)
    walker.walker_walk()
    return walker.get_state()


def walker_finalize(event, context, branch):
    walker = vis_obj(event, context)
    walker.walker_finalize()

    if len(walker.work_ids):
        walker._status = WalkerStatus.init.name
    else:
        walker._status = WalkerStatus.end.name

    return walker.get_state()


def walker_failed(event, context, branch):
    walker = vis_obj(event, context)
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
        "ErrorEquals": ["States.Timeout",
                        "Lambda.AWSLambdaException",
                        "Lambda.SdkClientException",
                        "Lambda.ServiceException"],
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
        "Next": next_state,
        "ResultPath": None
    }]


def walker_sfn(i):
    def _branch_call(func):
        def wrapped(event, context):
            return func(event, context, i)
        return wrapped

    return {
        'StartAt': f'IsActive{i}',
        'States': {
            f'IsActive{i}': {
                'Type': 'Choice',
                'Choices': [
                    {
                        'Variable': '$._number_of_workers',
                        'NumericLessThanEquals': i,
                        'Next': f'End{i}'
                    }
                ],
                'Default': f'CheckStatus{i}'
            },
            f'CheckStatus{i}': {
                'Type': 'Choice',
                'Choices': [
                    {
                        'Variable': '$._status',
                        'StringEquals': WalkerStatus.init.name,
                        'Next': f'Initialize{i}'
                    },
                    {
                        'Variable': '$._status',
                        'StringEquals': WalkerStatus.walk.name,
                        'Next': f'Walk{i}'
                    },
                    {
                        'Variable': '$._status',
                        'StringEquals': WalkerStatus.finished.name,
                        'Next': f'Finalize{i}'
                    },
                    {
                        'Variable': '$._status',
                        'StringEquals': WalkerStatus.end.name,
                        'Next': f'End{i}'
                    }
                ],
                'Default': f'Finalize{i}'
            },
            f'Initialize{i}': {
                'Type': 'Task',
                'Resource': _branch_call(walker_initialize),
                'Retry': _retry,
                'Catch': _catch_to_state(f'Failed{i}'),
                'Next': f'Walk{i}'
            },
            f'Walk{i}': {
                'Type': 'Task',
                'Resource': _branch_call(walker_walk),
                'Retry': _retry,
                'Catch': _catch_to_state(f'Failed{i}'),
                'TimeoutSeconds': 295,
                'Next': f'CheckStatus{i}'
            },
            f'Finalize{i}': {
                'Type': 'Task',
                'Resource': _branch_call(walker_finalize),
                'Retry': _retry,
                'Catch': _catch_to_state(f'Failed{i}'),
                'Next': f'CheckStatus{i}'
            },
            f'Failed{i}': {
                'Type': 'Task',
                'Resource': _branch_call(walker_failed),
                'Retry': _retry,
                'Catch': _catch_to_state(f'Fail{i}'),
                'Next': f'Fail{i}'
            },
            f'Fail{i}': {
                'Type': 'Fail',
            },
            f'End{i}': {
                'Type': 'Pass',
                'End': True,
                'OutputPath': '$.work_result'
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
            "Branches": [walker_sfn(i) for i in range(THREADPOOL_PARALLEL_FACTOR)],
            "Retry": _retry,
            "ResultPath": "$.work_result",
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

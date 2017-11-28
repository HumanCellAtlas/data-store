
import json
import time
import boto3
import string
import botocore
from uuid import uuid4
from enum import Enum, auto
from .utils import *
from dss import BucketConfig, Config
from .blobstore import BlobListerizer


Config.set_config(BucketConfig.NORMAL)


class DSSVisitationException(Exception):
    pass


class DSSVisitationExceptionSkipItem(DSSVisitationException):
    pass


class DSSVisitationExceptionRetry(DSSVisitationException):
    pass


class StatusCode(Enum):
    RUNNING = auto()
    SUCCEEDED = auto()
    FAILED = auto()


class Visitation:

    _base_state_spec = dict(
        visitation_class_name = str,
        code = str,
        replica = str,
        bucket = str,
        dirname = str,
    )


    sentinel_state_spec = dict(
        name = str,
        k_workers = int,
        waiting = list,
        wait_time = int,
    )


    walker_state_spec = dict(
        prefix = str,
        marker = str,
        token = str,
        k_processed = int,
        k_starts = int,
    )


    sentinel_arn = statefunction_arn('dss-visitation-sentinel')
    walker_arn = statefunction_arn('dss-visitation-walker')


    _walker_timeout = 240


    def __init__(self, state_spec, state):

        self.state_spec = self._base_state_spec.copy()

        self.state_spec.update(
            state_spec
        )

        for k, default in self.state_spec.items():
            v = state.get(k, None)

            if v is None:
                v = default()

            setattr(self, k, default(v))


    def propagate_state(self):

        return {
            k : getattr(self, k)
                for k in self.state_spec
        }


    @classmethod
    def sentinel_state(cls, state):

        return cls(
            cls.sentinel_state_spec,
            state
        )


    @classmethod
    def walker_state(cls, state):

        return cls(
            cls.walker_state_spec,
            state
        )


    @classmethod
    def get_status(cls, name):

        walker_executions, k_api_calls = list_executions_for_sentinel(
            name
        )

        running = [
            e['name'].split('--')[0] for e in walker_executions
                if e['status'] == StatusCode.RUNNING.name
        ]

        succeeded = [
            e['name'].split('--')[0] for e in walker_executions
                if e['status'] == StatusCode.SUCCEEDED.name
        ]

        failed = [
            e['name'].split('--')[0] for e in walker_executions
                if e['status'] == StatusCode.FAILED.name
        ]

        return {
            'running': running,
            'succeeded': succeeded,
            'failed': failed,
            'k_api_calls': k_api_calls
        }


    def muster(self):

        status = type(self).get_status(
            self.name
        )

        running = status['running']

        self.waiting = list(
            set(self.waiting) - set(running) - set(status['succeeded']) - set(status['failed'])
        )

        self.wait_time = 2 * status['k_api_calls']

        if len(running) < self.k_workers:
            k_new = min(
                self.k_workers - len(running),
                len(self.waiting)
            )

            for i in range(k_new):
                pfx = self.waiting[i]

                running.append(pfx)

                self.start_walker(pfx)

        if running:
            self.code = StatusCode.RUNNING.name
        else:
            self.code = StatusCode.SUCCEEDED.name

        return running


    def start_walker(self, pfx):

        name = f'{pfx}--{self.name}'

        resp = boto3.client('stepfunctions').start_execution(
            stateMachineArn = type(self).walker_arn,
            name = name,
            input = json.dumps({
                'visitation_class_name': self.visitation_class_name,
                'replica': self.replica,
                'bucket': self.bucket,
                'dirname': self.dirname,
                'prefix': pfx
            })
        )

        return resp


    def initialize(self):
        raise NotImplementedError


    def finalize(self):
        raise NotImplementedError


    def finalize_failed(self):
        raise NotImplementedError


    def initialize_walker(self):
        raise NotImplementedError


    def process_item(self, key):
        raise NotImplementedError


    def walk(self):
        raise NotImplementedError


    def finalize_walker(self):
        raise NotImplementedError


    def finalize_failed_walker(self):
        raise NotImplementedError


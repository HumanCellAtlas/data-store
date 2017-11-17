
import json
import boto3
import botocore
from time import time
from uuid import uuid4
from enum import Enum, auto
from .utils import *


class StatusCode(Enum):
    RUNNING = auto()
    SUCCEEDED = auto()
    FAILED = auto()


class _BaseRep:
    _props = dict()

    def __init__(self, logger=None, **kwargs):
        self.logger = logger

        for k, default in self._props.items():
            v = kwargs.get(k, None)

            if v is None:
                v = default()

            setattr(self, k, default(v))


    def to_dict(self, ** kwargs):
        d = {
            k : getattr(self, k)
                for k in self._props
        }

        d.update(
            ** kwargs
        )

        return d


class Sentinel(_BaseRep):
    _props = dict(
        name = str,
        replica = str,
        bucket = str,
        k_workers = int,
        waiting = list,
        wait_time = int,
        code = str,
    )

    ARN = statefunction_arn('dss-visitation-sentinel')

    @classmethod
    def get_status(cls, name):
        execution_arn = statefunction_arn(
            'dss-visitation-sentinel',
            name
        )

        walker_executions, k_api_calls = get_executions(
            Walker.ARN,
            get_start_date(
                execution_arn
            )
        )

        running = [
            e['name'].split('--')[0] for e in walker_executions
                if e['status'] == StatusCode.RUNNING.name
                and e['name'].endswith(name)
        ]

        succeeded = [
            e['name'].split('--')[0] for e in walker_executions
                if e['status'] == StatusCode.SUCCEEDED.name
                and e['name'].endswith(name)
        ]

        failed = [
            e['name'].split('--')[0] for e in walker_executions
                if e['status'] == StatusCode.FAILED.name
                and e['name'].endswith(name)
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

                walker = Walker(
                    ** self.to_dict(),
                    prefix = pfx
                )

                walker.start(
                    f'{pfx}--{self.name}'
                )

        if running:
            self.code = StatusCode.RUNNING.name
        else:
            self.code = StatusCode.SUCCEEDED.name


class Walker(_BaseRep):
    _props = dict(
        replica = str,
        bucket = str,
        prefix = str,
        marker = str,
        k_processed = int,
        k_starts = int,
        code = str,
    )

    ARN = statefunction_arn('dss-visitation-walker')

    timeout = 240

    def start(self, name):
        resp = boto3.client('stepfunctions').start_execution(
            stateMachineArn = self.ARN,
            name = name,
            input = json.dumps({
                'replica': self.replica,
                'bucket': self.bucket,
                'prefix': self.prefix,
            })
        )

        return resp

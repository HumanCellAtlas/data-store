import os
import traceback
import typing
from uuid import uuid4
from contextlib import AbstractContextManager

from cloud_blobstore import BlobStore

from dss.util.aws.clients import sqs, sts  # type: ignore
from concurrent.futures import ThreadPoolExecutor, as_completed


_account_id = sts.get_caller_identity()['Account']
_queue_url = "https://sqs.{}.amazonaws.com/{}/dss-operations-{}".format(os.environ['AWS_DEFAULT_REGION'],
                                                                        _account_id,
                                                                        os.environ['DSS_DEPLOYMENT_STAGE'])


class CommandForwarder(AbstractContextManager):
    """
    Context manager for forwarding commands for Lambda execution through batch SQS queuing
    """
    def __init__(self):
        self.chunk = list()

    def forward(self, msg: str):
        self.chunk.append(msg)
        if 10 == len(self.chunk):
            _enqueue_command_batch(self.chunk)
            self.chunk = list()

    def __exit__(self, *args, **kwargs):
        if len(self.chunk):
            _enqueue_command_batch(self.chunk)

def map_bucket_results(func: typing.Callable, handle: BlobStore, bucket: str, base_pfx: str, parallelization=16):
    """
    Call `func` on an iterable of keys
    func is expected to be thread safe.
    """
    with ThreadPoolExecutor(max_workers=parallelization) as e:
        futures = list()
        for pfx in "0123456789abcdef":
            f = e.submit(func, handle.list(bucket, prefix=f"{base_pfx}{pfx}"))
            futures.append(f)
        for f in as_completed(futures):
            try:
                yield f.result()
            except Exception:
                traceback.print_exc()

def map_bucket(*args, **kwargs):
    for _ in map_bucket_results(*args, **kwargs):
        pass

def _enqueue_command_batch(commands: typing.List[str]):
    assert 10 >= len(commands)
    resp = sqs.send_message_batch(
        QueueUrl=_queue_url,
        Entries=[dict(Id=str(uuid4()), MessageBody=command)
                 for command in commands]
    )
    return resp

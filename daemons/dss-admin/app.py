import inspect
import json
import os
import sys
from typing import Callable, Any, Mapping, Optional

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import BucketConfig, Config, Replica
from dss.logging import configure_lambda_logging
from dss.stepfunctions.visitation.index import IndexVisitation
from dss.stepfunctions.visitation.storage import StorageVisitation
from dss.util.types import JSON


Config.set_config(BucketConfig.NORMAL)


class Target:
    """
    The target of an admin operation. Targets are nouns (like index, storage) and actions are verbs (like verify,
    repair). After adding a subclass here, a corresponding subcommand (aka target) should be added to the admin CLI (
    admin-cli.py). Every method in the target subclass should correspond to an subsubcommand (aka action) of the
    target subcommand.
    """
    pass


class IndexTarget(Target):
    """
    Admin operations on the Elasticsearch index.
    """
    def __init__(self, replica: str, bucket: str = None, prefix: str = None) -> None:
        super().__init__()
        self.replica = Replica[replica]
        self.bucket = bucket or self.replica.bucket
        self.prefix = prefix or ''

    def repair(self, workers: int) -> JSON:
        return self._start(workers, dryrun=False, notify=False)

    def verify(self, workers: int) -> JSON:
        return self._start(workers, dryrun=True, notify=False)

    def _start(self, workers: int, dryrun: bool, notify: Optional[bool]) -> Mapping[str, Any]:
        assert 1 < workers
        return IndexVisitation.start(workers,
                                     replica=self.replica.name,
                                     bucket=self.bucket,
                                     prefix=self.prefix,
                                     dryrun=dryrun,
                                     notify=notify)


class StorageTarget(Target):
    """
    Admin operations on the storage buckets and the replication between them.
    """
    def __init__(self, replicas: Mapping[str, str], prefix: str = None) -> None:
        super().__init__()
        replicas = ((Replica[replica], bucket) for replica, bucket in replicas.items())
        self.replicas = {replica.name: bucket or replica.bucket for replica, bucket in replicas}
        self.prefix = prefix or ''

    def verify(self, workers: int, folder: str, quick: bool = False) -> JSON:
        assert 1 < workers
        replicas, buckets = zip(*self.replicas.items())
        return StorageVisitation.start(workers,
                                       replicas=replicas,
                                       buckets=buckets,
                                       prefix=self.prefix,
                                       folder=folder,
                                       quick=quick)


def _invoke(f: Callable, kwargs: Mapping[str, Any]) -> Any:
    """
    Invoke the given function with a given dictionary of keyword arguments, silently ignoring any extra arguments.
    """
    signature = inspect.signature(f)
    kwargs = {name: kwargs.get(name, param.default) for name, param in signature.parameters.items()}
    return f(**kwargs)


class DSSAdmin(domovoi.Domovoi):
    def __call__(self, options, context):
        target_class_name = options['target'].capitalize() + Target.__name__
        target_class = globals()[target_class_name]
        assert issubclass(target_class, Target)
        target = _invoke(target_class, options)
        action_name = options['action']
        action = getattr(target, action_name)
        result = _invoke(action, options)
        return json.dumps(result)


configure_lambda_logging()
app = DSSAdmin(configure_logs=False)

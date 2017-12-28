import argparse
import inspect

import os
import sys
import json
import domovoi
from typing import Callable, Any, Mapping

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import BucketConfig, Config, Replica
from dss.stepfunctions.visitation.reindex import Reindex


Config.set_config(BucketConfig.NORMAL)


class Target:
    """
    The target of an admin operation. After adding a subclass here, a corresponding subcommand (aka target) should be
    added to the admin CLI (admin-cli.py). Every method in the target subclass should correspond to an subsubcommand
    (aka action) of the target subcommand.
    """
    pass


class IndexTarget(Target):
    """
    Admin operations on the Elasticsearch index.
    """
    def __init__(self, replica: str, bucket: str = None) -> None:
        super().__init__()
        self.replica = Replica[replica]
        self.bucket = bucket or self.replica.bucket

    def repair(self, workers: int) -> Mapping[str, Any]:
        raise NotImplementedError()

    def verify(self, workers: int) -> Mapping[str, Any]:
        return self._reindex(workers)

    def _reindex(self, workers: int) -> Mapping[str, Any]:
        assert 1 < workers
        visitation_id = Reindex.start(self.replica.name, self.bucket, workers)
        return {'visitation_id': visitation_id}


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


app = DSSAdmin()

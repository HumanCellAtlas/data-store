import os
import sys
import json
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import BucketConfig, Config, Replica
from dss.stepfunctions.visitation.reindex import Reindex


Config.set_config(BucketConfig.NORMAL)


def kickoff_reindex(event, context):
    replica = Replica[event['replica']]
    bucket = event['bucket']
    number_of_workers = event['number_of_workers']

    assert 1 < number_of_workers
    # handle = Config.get_cloud_specific_handles(replica)[0]
    # assert handle.check_bucket_exists(bucket)

    name = Reindex.start(replica.name, bucket, number_of_workers)

    return json.dumps({
        'name': name
    })


class DSSBackdoor(domovoi.Domovoi):
    def __call__(self, event, context):
        return kickoff_reindex(event, context)


app = DSSBackdoor()

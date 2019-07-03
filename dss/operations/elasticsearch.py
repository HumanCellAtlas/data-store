"""
Elasticsearch tools
"""
import os
import json
import typing
import argparse
import logging

from dcplib.aws.sqs import SQSMessenger, get_queue_url

from dss import Replica
from dss.operations import dispatch


logger = logging.getLogger(__name__)


elasticsearch = dispatch.target("elasticsearch", help=__doc__)


@elasticsearch.action("index",
                      arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                                 "--keys": dict(default=None, nargs="*", help="keys to index.")})
def index(argv: typing.List[str], args: argparse.Namespace):
    """
    Initiate the indexer lambda for `keys`
    """
    index_queue_url = get_queue_url("dss-index-operation-" + os.environ['DSS_DEPLOYMENT_STAGE'])
    with SQSMessenger(index_queue_url) as sqsm:
        for key in args.keys:
            sqsm.send(json.dumps(dict(replica=args.replica, key=key)))

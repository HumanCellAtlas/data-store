import json
import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.events.chunkedtask import aws
from dss.events.chunkedtask import awsconstants

app = domovoi.Domovoi()

dss.Config.set_config(dss.BucketStage.NORMAL)

worker_sns_topic = awsconstants.get_worker_sns_topic()


@app.sns_topic_subscriber(worker_sns_topic)
def process_work(event: dict, context) -> None:
    payload = json.loads(event["Records"][0]["Sns"]["Message"])
    aws.dispatch(context, payload)

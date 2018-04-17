from concurrent.futures import ThreadPoolExecutor
import json
import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Replica
from dss.index import DEFAULT_BACKENDS
from dss.index.backend import CompositeIndexBackend
from dss.index.indexer import Indexer
from dss.logging import configure_lambda_logging

app = domovoi.Domovoi(configure_logs=False)
configure_lambda_logging()

dss.Config.set_config(dss.BucketConfig.NORMAL)

s3_bucket = dss.Config.get_s3_bucket()


@app.s3_event_handler(bucket=s3_bucket, events=["s3:ObjectCreated:*"])
def dispatch_s3_indexer_event(event, context) -> None:
    if event.get("Event") == "s3:TestEvent":
        app.log.info("DSS index daemon received S3 test event")
    else:
        _handle_event(Replica.aws, event, context)


@app.sns_topic_subscriber("dss-gs-bucket-events-" + os.environ["DSS_GS_BUCKET"])
def dispatch_gs_indexer_event(event, context):
    """
    This handler receives GS events via the Google Cloud Function deployed from daemons/dss-gs-event-relay.
    """
    gs_event = json.loads(event['Records'][0]['Sns']['Message'])
    event = gs_event['data']
    _handle_event(Replica.gcp, event, context)


def _handle_event(replica, event, context):
    executor = ThreadPoolExecutor(len(DEFAULT_BACKENDS))
    # We can't use ecxecutor as context manager because we don't want the shutdown to block
    try:
        backend = CompositeIndexBackend(executor, DEFAULT_BACKENDS)
        indexer_cls = Indexer.for_replica(replica)
        indexer = indexer_cls(backend, context)
        indexer.process_new_indexable_object(event)
    finally:
        executor.shutdown(False)

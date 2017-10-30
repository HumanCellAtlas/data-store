import os
from enum import Enum

sns_topics = dict(get_bundle="dss-checkout-get-bundle-" + os.environ["DSS_DEPLOYMENT_STAGE"],
                  copy_supervisor="dss-checkout-copy-supervisor-" + os.environ["DSS_DEPLOYMENT_STAGE"],
                  copy_worker="dss-checkout-copy-worker-" + os.environ["DSS_DEPLOYMENT_STAGE"],
                  closer="dss-s3-mpu-ready-" + os.environ["DSS_DEPLOYMENT_STAGE"])

class CopyParams:
    SOURCE_BUCKET = "source_bucket",
    SOURCE_KEY = "source_key",
    DESTINATION_BUCKET = "dest_bucket",
    DESTINATION_KEY = "dest_key"


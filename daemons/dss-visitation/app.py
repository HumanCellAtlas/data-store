import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import BucketConfig, Config
from dss.logging import configure_lambda_logging
from dss.stepfunctions.visitation.implementation import sfn


configure_lambda_logging()
app = domovoi.Domovoi(configure_logs=False)
Config.set_config(BucketConfig.NORMAL)

app.register_state_machine(sfn)

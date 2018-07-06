import logging
import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.logging import configure_lambda_logging
from dss.stepfunctions.checkout.checkout_states import state_machine_def

logger = logging.getLogger(__name__)

configure_lambda_logging()
app = domovoi.Domovoi(configure_logs=False)

dss.Config.set_config(dss.BucketConfig.NORMAL)

app.register_state_machine(state_machine_def)

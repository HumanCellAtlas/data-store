import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import Config
from dss.logging import configure_lambda_logging
from dss.notify.notifier import Notifier
from dss.util.time import RemainingLambdaContextTime, AdjustedRemainingTime
from dss.util.types import LambdaContext

configure_lambda_logging()


def deploy_notifier():
    notifier = Notifier.from_config()
    notifier.deploy()


app = domovoi.Domovoi(configure_logs=False)


@app.scheduled_function("rate(1 minute)", rule_name='run_notifier_' + Config.deployment_stage())
def run_notifier(event, context: LambdaContext):
    notifier = Notifier.from_config()
    shutdown_seconds = 5.0
    remaining_time = RemainingLambdaContextTime(context)
    adjusted_remaining_time = AdjustedRemainingTime(-shutdown_seconds, remaining_time)
    notifier.run(adjusted_remaining_time)

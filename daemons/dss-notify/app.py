import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.logging import configure_lambda_logging
from dss.notify.notifier import Notifier

configure_lambda_logging()


def deploy_notifier():
    notifier = Notifier.from_config()
    notifier.deploy()


app = domovoi.Domovoi(configure_logs=False)


@app.scheduled_function("rate(1 minute)")
def run_notifier(event, context):
    notifier = Notifier.from_config()
    notifier.run(context)

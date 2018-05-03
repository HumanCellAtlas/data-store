import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import Config
from dss.logging import configure_lambda_logging
from dss.notify.notifier import Notifier

configure_lambda_logging()


def deploy_notifier():
    notifier = Notifier.from_config()
    notifier.deploy()


app = domovoi.Domovoi(configure_logs=False)


def run_notifier(event, context):
    notifier = Notifier.from_config()
    notifier.run(context)


# FIXME: https://github.com/HumanCellAtlas/data-store/issues/1211
run_notifier.__name__ += "_" + Config.deployment_stage()
run_notifier = app.scheduled_function("rate(1 minute)")(run_notifier)

import json
import logging
import os
import sys

import boto3
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import stepfunctions
from dss.stepfunctions import SFN_TEMPLATE_KEY, SFN_EXECUTION_KEY, SFN_INPUT_KEY, sfn_sns_topic
from dss.logging import configure_daemon_logging

logger = logging.getLogger(__name__)
configure_daemon_logging()
app = domovoi.Domovoi(configure_logs=False)
sqs = boto3.resource('sqs')


@app.sns_topic_subscriber(sfn_sns_topic)
def launch_sfn_run(event, context):
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    sfn_name_template = msg[SFN_TEMPLATE_KEY]
    sfn_execution = msg[SFN_EXECUTION_KEY]
    sfn_input = msg[SFN_INPUT_KEY]
    logger.info(f"Launching Step Function {sfn_name_template} execution: {sfn_execution} input: {str(sfn_input)}")
    try:
        response = stepfunctions._step_functions_start_execution(sfn_name_template, sfn_execution, sfn_input)
        logger.info(f"Started step function execution: {str(response)}")
    except Exception as e:
        logger.warning(f"Failed to start step function execution: {str(e)}")
        raise e

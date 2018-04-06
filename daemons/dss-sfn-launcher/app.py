import json
import logging
import os
import random
import sys

import boto3
from botocore.exceptions import ClientError
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
    sns_msg = event["Records"][0]["Sns"]
    logger.debug(f'sns_message: {str(sns_msg)}')
    msg = json.loads(sns_msg["Message"])
    attrs = sns_msg["MessageAttributes"]

    if 'DSS-REAPER-RETRY-COUNT' in attrs:
        logger.info(f"Reprocessing attempts so far {attrs['DSS-REAPER-RETRY-COUNT']['Value']}")

    sfn_name_template = msg[SFN_TEMPLATE_KEY]
    sfn_execution = msg[SFN_EXECUTION_KEY]
    sfn_input = msg[SFN_INPUT_KEY]
    logger.debug(f"Launching Step Function {sfn_name_template} execution: {sfn_execution} input: {str(sfn_input)}")
    try:
        response = stepfunctions._step_functions_start_execution(sfn_name_template, sfn_execution, sfn_input)
        logger.debug(f"Started step function execution: {str(response)}")
    except ClientError as e:
        if e.response.get('Error'):
            if e.response['Error'].get('Code') == 'ExecutionAlreadyExists':
                logger.warning(f"Execution id {sfn_execution} already exists for {sfn_name_template}. Not retrying.")
            else:
                logger.warning(f"Failed to start step function execution id {sfn_execution}: {str(e)}")
                raise e

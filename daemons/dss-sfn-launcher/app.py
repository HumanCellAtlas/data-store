import json
import logging
import os
import sys

import boto3
import domovoi
from botocore.exceptions import ClientError

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import stepfunctions
from dss.stepfunctions import SFN_TEMPLATE_KEY, SFN_EXECUTION_KEY, SFN_INPUT_KEY, sfn_sns_topic
from dss.util import tracing
from dss.logging import configure_lambda_logging

logger = logging.getLogger(__name__)
configure_lambda_logging()
app = domovoi.Domovoi(configure_logs=False)
sqs = boto3.resource('sqs')

@app.sns_topic_subscriber(sfn_sns_topic)
def launch_sfn_run(event, context):
    sns_msg = event["Records"][0]["Sns"]
    logger.debug(f'sns_message: {sns_msg}')
    msg = json.loads(sns_msg["Message"])
    attrs = sns_msg["MessageAttributes"]

    if 'DSS-REAPER-RETRY-COUNT' in attrs:
        logger.info("Reprocessing attempts so far %s", attrs['DSS-REAPER-RETRY-COUNT']['Value'])

    sfn_name_template = msg[SFN_TEMPLATE_KEY]
    sfn_execution = msg[SFN_EXECUTION_KEY]
    sfn_input = msg[SFN_INPUT_KEY]
    logger.debug("Launching Step Function %s execution: %s input: %s}", sfn_name_template, sfn_execution, sfn_input)
    try:
        response = stepfunctions._step_functions_start_execution(sfn_name_template, sfn_execution, sfn_input)
        logger.debug(f"Started step function execution: %s", str(response))
    except ClientError as e:
        if e.response.get('Error'):
            if e.response['Error'].get('Code') == 'ExecutionAlreadyExists':
                logger.warning("Execution id %s already exists for %s. Not retrying.", sfn_execution, sfn_name_template)
            else:
                logger.warning("Failed to start step function execution id %s: due to %s", sfn_execution, str(e))
                raise e

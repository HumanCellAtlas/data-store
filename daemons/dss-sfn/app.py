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
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

DSS_REAPER_RETRY_KEY = 'DSS-REAPER-RETRY-COUNT'
DSS_MAX_RETRY_COUNT = 10
configure_daemon_logging()
app = domovoi.Domovoi(configure_logs=False)
sqs = boto3.resource('sqs')


@app.sns_topic_subscriber(sfn_sns_topic)
def launch_test_run(event, context):
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    sfn_name_template = msg[SFN_TEMPLATE_KEY]
    sfn_execution = msg[SFN_EXECUTION_KEY]
    sfn_input = msg[SFN_INPUT_KEY]
    logger.info(f"Launching Step Function {sfn_name_template} execution: {sfn_execution} input: {str(sfn_input)}")
    stepfunctions._step_functions_start_execution(sfn_name_template, sfn_execution, sfn_input)


@app.scheduled_function("rate(1 minute)")
def reaper(event, context):
    queue_name = "dss-dlq-sfn-" + os.environ["DSS_DEPLOYMENT_STAGE"]

    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    for message in queue.receive_messages():
        # re-process messages by sending them back to the SNS topic
        sns_message = json.loads(message.body)["Records"][0]["Sns"]
        msg = sns_message["Message"]
        logger.debug(f"Received a message: {str(msg)}")

        msg_dict = json.loads(msg)
        sfn_name_template = msg_dict[SFN_TEMPLATE_KEY]
        sfn_execution = msg_dict[SFN_EXECUTION_KEY]
        sfn_input = json.loads(msg_dict[SFN_INPUT_KEY])
        logger.debug(f"sfn_input: {str(sfn_input)}")

        attrs = sns_message["MessageAttributes"]
        retry_count = int(attrs[DSS_REAPER_RETRY_KEY]['Value']) if DSS_REAPER_RETRY_KEY in attrs else 0
        retry_count += 1
        if retry_count < DSS_MAX_RETRY_COUNT:
            attrs = {DSS_REAPER_RETRY_KEY: {"DataType": "Number", "StringValue": str(retry_count)}}
            logger.debug(f"Incremented retry count: {retry_count}")

            logger.debug(f"Schedule {sfn_name_template} reprocessing execution: {sfn_execution}")
            stepfunctions.step_functions_invoke(sfn_name_template, sfn_execution, sfn_input, attrs)
        else:
            logger.warning(f"Giving up on executionid: {sfn_execution} after {retry_count} attempts")
            break

        # Let the queue know that the message is processed
        message.delete()

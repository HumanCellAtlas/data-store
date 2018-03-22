import json
import logging
import os
import random
import sys

import boto3
import domovoi
from botocore.exceptions import ClientError

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
def launch_test_run(event, context):
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    sfn_name_template = msg[SFN_TEMPLATE_KEY]
    sfn_execution = msg[SFN_EXECUTION_KEY]
    sfn_input = msg[SFN_INPUT_KEY]
    logger.info(f"Launching Step Function {sfn_name_template} execution: {sfn_execution}")
    #    if random.randint(0, 100) < 20:
    #        print(f"Raised exception execution id {sfn_execution}")
    print('About to throw an expection')
    raise Exception()
    # print(f"Starting execution {sfn_execution}")
    # stepfunctions._step_functions_start_execution(sfn_name_template, sfn_execution, sfn_input)


@app.scheduled_function("rate(1 minute)")
def reaper(event, context):
    queue_name = "dss-dlq-sfn-" + os.environ["DSS_DEPLOYMENT_STAGE"]
    print(f"Queue name: {queue_name}")

    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    print('Got a queue')

    try:
        for message in queue.receive_messages():
            print("Got a message: ")
            print(str(message.body))
            # re-process messages by sending them back to the SNS topic
            msg = json.loads(message.body)["Records"][0]["Sns"]["Message"]
            print("Message: " + str(msg))
            msg_dict = json.loads(msg)
            sfn_name_template = msg_dict[SFN_TEMPLATE_KEY]
            sfn_execution = msg_dict[SFN_EXECUTION_KEY]
            sfn_input = msg_dict[SFN_INPUT_KEY]
            logger.info(f"Schedule {sfn_name_template} reprocessing execution: {sfn_execution}")
            # TODO(rkisin): increment retry count
            stepfunctions.step_functions_invoke(sfn_name_template, sfn_execution, sfn_input)
            # Let the queue know that the message is processed
            message.delete()
    except Exception as e:
        print("Error: %s" % str(e))
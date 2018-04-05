import json
import logging
import os
import sys

import boto3
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.logging import configure_daemon_logging
from dss.util.aws import send_sns_msg

logger = logging.getLogger(__name__)
configure_daemon_logging()

DSS_REAPER_RETRY_KEY = 'DSS-REAPER-RETRY-COUNT'
# Max number of retries per message before we give up
DSS_MAX_RETRY_COUNT = 10

# Number of times messages are picked from the DLQ per execution (per minute)
RECEIVE_BATCH_COUNT = 10

app = domovoi.Domovoi(configure_logs=False)
sqs = boto3.resource('sqs')


@app.scheduled_function("rate(1 minute)")
def reaper(event, context):
    queue_name = "dss-dlq-" + os.environ["DSS_DEPLOYMENT_STAGE"]

    # Get the queue
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    message_count = 0

    while context.get_remaining_time_in_millis() > 10000:
        for message in queue.receive_messages(MaxNumberOfMessages=10, AttributeNames=['All'],
                                              MessageAttributeNames=['All']):
            try:
                logger.debug(f"Received a message for reprocessing: {str(message.body)}")
                # re-process messages by sending them back to the SNS topic
                sns_message = json.loads(message.body)["Records"][0]["Sns"]
                topic_arn = sns_message["TopicArn"]
                msg = json.loads(sns_message["Message"])
                logger.info(f"Received a message for reprocessing: {str(msg)} SNS topic ARN: {topic_arn}")

                attrs = sns_message["MessageAttributes"]
                retry_count = int(attrs[DSS_REAPER_RETRY_KEY]['Value']) if DSS_REAPER_RETRY_KEY in attrs else 0
                retry_count += 1
                if retry_count < DSS_MAX_RETRY_COUNT:
                    attrs = {DSS_REAPER_RETRY_KEY: {"DataType": "Number", "StringValue": str(retry_count)}}
                    logger.info(f"Incremented retry count: {retry_count} and resend SNS message")
                    send_sns_msg(topic_arn, msg, attrs)
                else:
                    logger.critical(f"Giving up on message: {msg} after {retry_count} attempts")
            except Exception as e:
                logger.error(f"Unable to process message: {str(message)} due to {str(e)}")

            # Let the queue know that the message is processed
            logger.info('Deleting message from the queue')
            message.delete()
            message_count += 1

    logger.info(f"Processed {message_count} messages")

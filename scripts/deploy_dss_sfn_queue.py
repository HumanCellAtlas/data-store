#!/usr/bin/env python

import boto3
import os

sqs = boto3.resource('sqs')

# Create the queue. This returns an SQS.Queue instance
# Amazon SQS returns  error only if the request includes attributes whose values differ from
# those of the existing queue.
queue = sqs.create_queue(QueueName='dss-dlq-sfn-' + os.environ["DSS_DEPLOYMENT_STAGE"], Attributes={'DelaySeconds': '5'})

print(f"Queue: {queue.url}")

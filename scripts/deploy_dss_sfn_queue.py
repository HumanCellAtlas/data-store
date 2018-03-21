#!/usr/bin/env python

import boto3
import os

sqs = boto3.resource('sqs')

# Create the queue. This returns an SQS.Queue instance
queue = sqs.create_queue(QueueName='dss-dlq-sfn-'+os.environ["DSS_DEPLOYMENT_STAGE"], Attributes={'DelaySeconds': '5'})

# You can now access identifiers and attributes
print(queue.url)
print(queue.attributes.get('DelaySeconds'))
import time

import boto3
import botocore.exceptions


def log_message(log_group_name: str, log_stream_name: str, message: str):
    """Logs a message to cloudwatch."""

    logs_client = boto3.client("logs")

    def get_sequence_token():
        # try to get the upload sequence token
        paginator = logs_client.get_paginator('describe_log_streams')
        for page in paginator.paginate(logGroupName=log_group_name, logStreamNamePrefix=log_stream_name):
            for log_stream in page['logStreams']:
                if log_stream['logStreamName'] == log_stream_name:
                    return log_stream.get('uploadSequenceToken', None)

        return None

    while True:
        try:
            logs_client.create_log_group(logGroupName=log_group_name)
        except logs_client.exceptions.ResourceAlreadyExistsException:
            pass
        try:
            logs_client.create_log_stream(
                logGroupName=log_group_name, logStreamName=log_stream_name)
        except logs_client.exceptions.ResourceAlreadyExistsException:
            pass

        sequence_token = get_sequence_token()

        try:
            kwargs = dict(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                logEvents=[dict(
                    timestamp=int(time.time() * 1000),
                    message=message,
                )],
            )
            if sequence_token is not None:
                kwargs['sequenceToken'] = sequence_token

            logs_client.put_log_events(**kwargs)
            break
        except logs_client.exceptions.InvalidSequenceTokenException:
            pass

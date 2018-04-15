#!/usr/bin/env python

from datetime import datetime
import os
import sys
import time
import unittest
import uuid

import boto3

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util.aws import cloudwatch_logging
from tests.infra import testmode

class TestCloudwatchLogging(unittest.TestCase):
    @testmode.standalone
    def test_aws_logging_new_stream(self):
        stream_name = str(uuid.uuid4())
        cloudwatch_logging.log_message("dss-test-logging", stream_name, "hello world")

        logs_client = boto3.client("logs")
        starttime = time.time()
        while time.time() < starttime + 30:
            response = logs_client.filter_log_events(
                logGroupName="dss-test-logging", logStreamNames=[stream_name])

            for event in response['events']:
                if event['message'] == "hello world":
                    break
            else:
                continue
            break
        else:
            self.fail("Did not find message in logs")

    @testmode.standalone
    def test_aws_logging_fixed_stream(self):
        stream_name = "test_stream"
        message1, message2 = str(uuid.uuid4()), str(uuid.uuid4())
        cloudwatch_logging.log_message("dss-test-logging", stream_name, message1)
        ten_min_ago_in_ms_since_1970_utc = int((time.time() - 600) * 1000)

        logs_client = boto3.client("logs")
        starttime = time.time()
        while time.time() < starttime + 30:
            response = logs_client.filter_log_events(logGroupName="dss-test-logging",
                                                     logStreamNames=[stream_name],
                                                     startTime=ten_min_ago_in_ms_since_1970_utc)

            for event in response['events']:
                if event['message'] == message1:
                    break
            else:
                continue
            break
        else:
            self.fail("Did not find message in logs")

        cloudwatch_logging.log_message("dss-test-logging", stream_name, message2)

        starttime = time.time()
        while time.time() < starttime + 30:
            response = logs_client.filter_log_events(logGroupName="dss-test-logging",
                                                     logStreamNames=[stream_name],
                                                     startTime=ten_min_ago_in_ms_since_1970_utc)

            for event in response['events']:
                if event['message'] == message2:
                    break
            else:
                time.sleep(1)
                continue
            break
        else:
            self.fail("Did not find message in logs")


if __name__ == '__main__':
    unittest.main()

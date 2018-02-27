#!/usr/bin/env python
import json
import os

import boto3
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util.aws import ARN
from dss.stepfunctions import step_functions_arn

stage = os.environ.get('DSS_DEPLOYMENT_STAGE')
dev_stage = 'dev'
region = ARN.get_region()
accountid = ARN.get_account_id()

checkout_bundle_arn_prefix = f"arn:aws:lambda:{region}:{accountid}:function:dss-scalability-test-roman:domovoi-stepfunctions-task-CheckoutBundle"
upload_bundle_arn_prefix = f"arn:aws:lambda:{region}:{accountid}:function:dss-scalability-test-roman:domovoi-stepfunctions-task-UploadBundle"
download_bundle_arn_prefix = f"arn:aws:lambda:{region}:{accountid}:function:dss-scalability-test-roman:domovoi-stepfunctions-task-DownloadBundle"
dss_s3_copy_arn = f"arn:aws:states:{region}:{accountid}:stateMachine:dss-s3-copy-sfn-{dev_stage}"


def get_metrics_array(arn_template, cnt):
    metrics_array = [["AWS/States", "LambdaFunctionRunTime", "LambdaFunctionArn", arn_template + '{}'.format(0),
                      {"yAxis": "left", "period": 10}]]
    for idx in range(1, cnt):
        metrics_array.append(
            ["...",
             arn_template + '{}'.format(idx),
             {"period": 10}])
    return metrics_array


dashboard_def = {
    "widgets": [
        {
            "type": "metric",
            "x": 0,
            "y": 24,
            "width": 12,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "stacked": True,
                "metrics": [
                    ["AWS/ES", "CPUUtilization", "DomainName", f"dss-index-{stage}", "ClientId", "861229788715",
                     {"period": 10}],
                    [".", "WriteThroughput", ".", ".", ".", ".", {"period": 10}],
                    [".", "WriteIOPS", ".", ".", ".", ".", {"period": 10}]
                ],
                "region": region,
                "period": 300,
                "title": "Elastic search"
            }
        }
        ,
        {
            "type": "metric",
            "x": 0,
            "y": 0,
            "width": 15,
            "height": 3,
            "properties": {
                "view": "singleValue",
                "metrics": [
                    ["AWS/States", "ExecutionsStarted", "StateMachineArn",
                     step_functions_arn(f"stateMachine:dss-scalability-test-{stage}"),
                     {"stat": "Sum", "period": 3600}],
                    [".", "ExecutionsSucceeded", ".", ".", {"stat": "Sum", "period": 3600}],
                    [".", "ExecutionsFailed", ".", ".", {"stat": "Sum", "period": 3600}],
                    [".", "ExecutionThrottled", ".", ".", {"stat": "Sum", "period": 3600}]
                ],
                "region": region,
                "title": "Active tests",
                "period": 300
            }
        }
        ,
        {
            "type": "metric",
            "x": 0,
            "y": 12,
            "width": 18,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/States", "ExecutionsStarted", "StateMachineArn",
                     step_functions_arn(f"stateMachine:dss-scalability-test-{stage}"), {"stat": "Sum"}],
                    [".", "ExecutionsFailed", ".", ".", {"stat": "Sum"}],
                    [".", "ExecutionsTimedOut", ".", ".", {"stat": "Sum"}],
                    [".", "ExecutionThrottled", ".", ".", {"stat": "Sum"}],
                    [".", "ExecutionsSucceeded", ".", ".", {"stat": "Sum"}]
                ],
                "region": region,
                "title": "Tests",
                "period": 300
            }
        }
        ,
        {
            "type": "metric",
            "x": 12,
            "y": 24,
            "width": 6,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DynamoDB", "ConsumedWriteCapacityUnits", "TableName", "scalability_test"],
                    [".", "ConsumedReadCapacityUnits", ".", "."]
                ],
                "region": region,
                "title": "Dynamo DB"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 9,
            "width": 24,
            "height": 3,
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": get_metrics_array(checkout_bundle_arn_prefix, 10),
                "region": region,
                "title": "Checkout bundle runtime",
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 18,
            "width": 6,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "metrics": [
                    ["AWS/States", "LambdaFunctionsStarted", "LambdaFunctionArn",
                     upload_bundle_arn_prefix,
                     {"stat": "Sum"}],
                    [".", "LambdaFunctionsFailed", ".", ".", {"stat": "Sum"}],
                    [".", "LambdaFunctionsSucceeded", ".", ".", {"stat": "Sum"}],
                    [".", "LambdaFunctionsHeartbeatTimedOut", ".", ".", {"stat": "Sum"}]
                ],
                "region": region,
                "stacked": False,
                "title": "Upload bundle",
                "period": 300
            }
        }
        ,
        {
            "type": "metric",
            "x": 6,
            "y": 18,
            "width": 6,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "metrics": [
                    ["AWS/States", "LambdaFunctionsStarted", "LambdaFunctionArn",
                     download_bundle_arn_prefix,
                     {"stat": "Sum"}],
                    [".", "LambdaFunctionsFailed", ".", ".", {"stat": "Sum"}],
                    [".", "LambdaFunctionsSucceeded", ".", ".", {"stat": "Sum"}],
                    [".", "LambdaFunctionsHeartbeatTimedOut", ".", ".", {"stat": "Sum"}]
                ],
                "region": region,
                "stacked": False,
                "title": "Download bundle",
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 12,
            "y": 18,
            "width": 6,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "metrics": [
                    ["AWS/States", "LambdaFunctionsStarted", "LambdaFunctionArn",
                     checkout_bundle_arn_prefix,
                     {"stat": "Sum"}],
                    [".", "LambdaFunctionsFailed", ".", ".", {"stat": "Sum"}],
                    [".", "LambdaFunctionsSucceeded", ".", ".", {"stat": "Sum"}],
                    [".", "LambdaFunctionsHeartbeatTimedOut", ".", ".", {"stat": "Sum"}]
                ],
                "region": region,
                "stacked": False,
                "title": "Checkout bundle",
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 3,
            "width": 24,
            "height": 3,
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": get_metrics_array(upload_bundle_arn_prefix, 10),
                "region": region,
                "title": "Bundle upload runtime",
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 6,
            "width": 24,
            "height": 3,
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/States", "ExecutionTime", "StateMachineArn",
                     "arn:aws:states:us-east-1:861229788715:stateMachine:dss-s3-copy-sfn-dev", {"period": 10}]
                ],
                "region": region,
                "title": "Copy Execution Time",
                "period": 300
            }
        }
    ]
}

client = boto3.client('cloudwatch')

response = client.put_dashboard(
    DashboardName=f"Scaalbility-{stage}",
    DashboardBody=json.dumps(dashboard_def)
)

print(str(response))

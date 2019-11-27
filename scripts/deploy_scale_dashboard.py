#!/usr/bin/env python
"""
Assemble and deploy an AWS CloudWatch dashboard for scale tests
"""
import json
import os
import boto3
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.util.aws import ARN
from dss.stepfunctions import step_functions_arn

stage = os.environ.get('DSS_DEPLOYMENT_STAGE')
region = ARN.get_region()
accountid = ARN.get_account_id()

checkout_bundle_arn_prefix = f"arn:aws:lambda:{region}:{accountid}:function:dss-scalability-test-{stage}:domovoi-stepfunctions-task-CheckoutBundle"
upload_bundle_arn_prefix = f"arn:aws:lambda:{region}:{accountid}:function:dss-scalability-test-{stage}:domovoi-stepfunctions-task-UploadBundle"
download_bundle_arn_prefix = f"arn:aws:lambda:{region}:{accountid}:function:dss-scalability-test-{stage}:domovoi-stepfunctions-task-DownloadBundle"
dss_s3_copy_arn = f"arn:aws:states:{region}:{accountid}:stateMachine:dss-s3-copy-sfn-{stage}"
gs_copy_sfn_arn = f"arn:aws:states:{region}:{accountid}:stateMachine:dss-gs-copy-sfn-{stage}"
gs_copy_write_metadata_sfn_arn = f"arn:aws:states:{region}:{accountid}:stateMachine:dss-gs-copy-write-metadata-sfn-{stage}"
dss_s3_copy_write_metadata_arn = f"arn:aws:states:{region}:{accountid}:stateMachine:dss-s3-copy-write-metadata-sfn-{stage}"
dss_scalability_test_arn = f"arn:aws:states:{region}:{accountid}:stateMachine:dss-scalability-test-{stage}"

sfn_arns = [checkout_bundle_arn_prefix, gs_copy_sfn_arn, gs_copy_write_metadata_sfn_arn, dss_s3_copy_arn,
            dss_s3_copy_write_metadata_arn, dss_scalability_test_arn]

LAMBDA_METRIC_RUNTIME = "LambdaFunctionRunTime"
LAMBDA_METRIC_FAILED = "LambdaFunctionsFailed"
full_width = 18


def get_metrics_array(arn_template, metric, cnt):
    metrics_array = [["AWS/States", metric, "LambdaFunctionArn", arn_template + '{}'.format(0),
                      {"yAxis": "left", "period": 10}]]
    for idx in range(1, cnt):
        metrics_array.append(
            ["...",
             arn_template + '{}'.format(idx),
             {"period": 10}])
    return metrics_array


def get_metrics_array_sfn(arn_templates, metric):
    metrics_array = [["AWS/States", metric, "StateMachineArn", arn_templates[0],
                      {"stat": "Sum"}]]
    for arn in arn_templates[1:]:
        metrics_array.append(
            ["...",
             arn,
             {"stat": "Sum"}])
    return metrics_array


dashboard_def = {
    "widgets": [
        {
            "type": "metric",
            "x": 0,
            "y": 0,
            "width": full_width,
            "height": 3,
            "properties": {
                "view": "singleValue",
                "metrics": [
                    ["AWS/States", "ExecutionsStarted", "StateMachineArn",
                     step_functions_arn(f"dss-scalability-test-{stage}"),
                     {"stat": "Sum", "period": 3600}],
                    [".", "ExecutionsSucceeded", ".", ".", {"stat": "Sum", "period": 3600}],
                    [".", "ExecutionsFailed", ".", ".", {"stat": "Sum", "period": 3600}],
                    [".", "ExecutionThrottled", ".", ".", {"stat": "Sum", "period": 3600}]
                ],
                "region": region,
                "title": "Active tests",
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 1,
            "width": full_width,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "metrics": get_metrics_array_sfn(sfn_arns, 'ExecutionsStarted'),
                "region": region,
                "title": "SFN started",
                "period": 300,
                "stacked": True
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 2,
            "width": full_width,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "metrics": get_metrics_array_sfn(sfn_arns, 'ExecutionThrottled'),
                "region": region,
                "title": "SFN throttled",
                "period": 300,
                "stacked": True
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 3,
            "width": full_width,
            "height": 6,
            "styles": "undefined",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/SQS", "ApproximateNumberOfMessagesNotVisible", "QueueName", f"dss-dlq-{stage}"],
                    [".", "ApproximateNumberOfMessagesVisible", ".", "."],
                    [".", "NumberOfMessagesReceived", ".", "."],
                    [".", "NumberOfMessagesDeleted", ".", "."],
                    [".", "NumberOfMessagesSent", ".", "."]
                ],
                "region": region,
                "title": "DLQ"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 4,
            "width": 18,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/Lambda", "ConcurrentExecutions", {"period": 300, "stat": "Average"}],
                    [".", "Errors", {"period": 300, "stat": "Average"}],
                    [".", "Invocations", {"period": 300, "stat": "Average"}],
                    [".", "Throttles", {"period": 300, "stat": "Average"}],
                    [".", "UnreservedConcurrentExecutions", {"period": 300, "stat": "Average"}]
                ],
                "region": region,
                "period": 300,
                "title": "All Lambdas"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 5,
            "width": full_width,
            "height": 6,
            "styles": "undefined",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/Lambda", "Invocations", "FunctionName", "cwl_firehose_subscriber",
                     {"stat": "Sum", "period": 1}],
                    ["...", f"dss-checkout-sfn-{stage}", {"stat": "Sum", "period": 1}],
                    ["...", f"dss-dlq-reaper-{stage}", {"stat": "Sum", "period": 1}],
                    ["...", f"dss-index-{stage}", {"stat": "Sum", "period": 1}],
                    ["...", f"dss-{stage}", {"stat": "Sum", "period": 1}],
                    ["...", f"dss-s3-copy-sfn-{stage}", {"stat": "Sum", "period": 1}],
                    ["...", f"dss-s3-copy-write-metadata-sfn-{stage}", {"stat": "Sum", "period": 1}],
                    ["...", f"dss-scalability-test-{stage}", {"stat": "Sum", "period": 1}],
                    ["...", f"dss-sfn-launcher-{stage}", {"stat": "Sum", "period": 1}],
                    ["...", f"dss-sfn-{stage}", {"stat": "Sum", "period": 1}],
                    ["...", f"dss-sync-{stage}", {"stat": "Sum", "period": 1}],
                    ["...", "Firehose-CWL-Processor", {"stat": "Sum", "period": 1}]
                ],
                "region": region,
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 6,
            "width": full_width,
            "height": 6,
            "styles": "undefined",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/Lambda", "Duration", "FunctionName", "dss-checkout-sfn-roman"],
                    ["...", f"dss-dlq-reaper-{stage}"],
                    ["...", f"dss-gs-copy-sfn-{stage}"],
                    ["...", f"dss-gs-copy-write-metadata-sfn-{stage}"],
                    ["...", f"dss-index-{stage}"],
                    ["...", f"dss-{stage}"],
                    ["...", f"dss-s3-copy-sfn-{stage}"],
                    ["...", f"dss-s3-copy-write-metadata-sfn-{stage}"],
                    ["...", f"dss-scalability-test-{stage}"],
                    ["...", f"dss-sfn-launcher-{stage}"],
                    ["...", f"dss-sfn-{stage}"],
                    ["...", f"dss-sync-{stage}"]
                ],
                "region": region,
                "title": "Lambda duration"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 7,
            "width": full_width,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "stacked": True,
                "metrics": [
                    ["AWS/ApiGateway", "Count", "ApiName", "dss", "Resource", "/v1/bundles/{uuid}/checkout", "Stage",
                     stage, "Method", "POST", {"stat": "Sum", "period": 60}],
                    ["...", "/v1/bundles/checkout/{checkout_job_id}", ".", ".", ".", "GET",
                     {"stat": "Sum", "period": 60}],
                    ["...", "/v1/bundles/{uuid}", ".", ".", ".", "PUT", {"stat": "Sum", "period": 60}],
                    ["...", "/v1/files/{uuid}", ".", ".", ".", "HEAD", {"stat": "Sum", "period": 60}],
                    ["...", "/v1/bundles/{uuid}", ".", ".", ".", "GET", {"stat": "Sum", "period": 60}],
                    ["...", "/v1/files/{uuid}", ".", ".", ".", "PUT", {"stat": "Sum", "period": 60}],
                    ["...", "GET", {"stat": "Sum", "period": 60}],
                    ["...", "/v1/swagger.json", ".", ".", ".", ".", {"stat": "Sum", "period": 60}]
                ],
                "region": region,
                "title": "API Gateway",
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 8,
            "width": full_width,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "stacked": True,
                "metrics": [
                    ["AWS/ApiGateway", "Latency", "ApiName", "dss", "Resource",
                     "/v1/bundles/checkout/{checkout_job_id}", "Stage", stage, "Method", "GET"],
                    ["...", "/v1/bundles/{uuid}/checkout", ".", ".", ".", "POST"],
                    ["...", "/v1/files/{uuid}", ".", ".", ".", "PUT"],
                    ["...", "GET"],
                    ["...", "HEAD"],
                    ["...", "/v1/bundles/{uuid}", ".", ".", ".", "PUT"],
                    ["...", "/v1/swagger.json", ".", ".", ".", "GET"],
                    ["...", "/v1/bundles/{uuid}", ".", ".", ".", "."]
                ],
                "region": region,
                "title": "API Gateway (Latency)",
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 9,
            "width": full_width,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/States", "ExecutionsStarted", "StateMachineArn",
                     step_functions_arn(f"dss-scalability-test-{stage}"), {"stat": "Sum"}],
                    [".", "ExecutionsFailed", ".", ".", {"stat": "Sum"}],
                    [".", "ExecutionsTimedOut", ".", ".", {"stat": "Sum"}],
                    [".", "ExecutionThrottled", ".", ".", {"stat": "Sum"}],
                    [".", "ExecutionsSucceeded", ".", ".", {"stat": "Sum"}]
                ],
                "region": region,
                "title": "Tests",
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 10,
            "width": full_width,
            "height": 3,
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": get_metrics_array(upload_bundle_arn_prefix, LAMBDA_METRIC_RUNTIME, 10),
                "region": region,
                "title": "Bundle upload runtime",
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 11,
            "width": full_width,
            "height": 3,
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/States", "ExecutionTime", "StateMachineArn",
                     dss_s3_copy_arn, {"period": 10}]
                ],
                "region": region,
                "title": "Copy Execution Time",
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 12,
            "width": full_width,
            "height": 3,
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": get_metrics_array(checkout_bundle_arn_prefix, LAMBDA_METRIC_RUNTIME, 10),
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
                "metrics": get_metrics_array(upload_bundle_arn_prefix, LAMBDA_METRIC_FAILED, 10),
                "region": "us-east-1",
                "stacked": True,
                "title": "Upload bundle failures",
                "period": 300
            }
        },
        {
            "type": "metric",
            "x": 6,
            "y": 18,
            "width": 6,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "metrics": get_metrics_array(download_bundle_arn_prefix, LAMBDA_METRIC_FAILED, 10),
                "region": region,
                "stacked": True,
                "title": "Download bundle failures",
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
                "metrics": get_metrics_array(checkout_bundle_arn_prefix, LAMBDA_METRIC_FAILED, 10),
                "region": region,
                "stacked": True,
                "title": "Checkout bundle failures",
                "period": 300
            }
        },
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
        },
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
        }

    ]
}

client = boto3.client('cloudwatch')

response = client.put_dashboard(
    DashboardName=f"Scalability-{stage}",
    DashboardBody=json.dumps(dashboard_def)
)

print(f"Dashboard deployment status: {response['ResponseMetadata']['HTTPStatusCode']} ")

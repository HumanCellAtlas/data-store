# HCA DSS: Dead-letter queue based retry framework

This daemon is a part of the DLQ-based framework for reprocessing failed Lambda invokations

#### About the DLQ retry framework

1. The retry framework is based on the DLQ support built into AWS Lambda. When Lambda invocation fails the message that
triggered the invocation is placed into DLQ
2. The daemon is triggred by cloudwatch cron every minute
3. The daemon retrieves messages from SQS queue  dss-dlq-{stage}
4. The SNS messages are resent to the original SNS topic ARN with original payload
6. In order to track a number of attempts to reprocess a message is recorded into SNS message attribute named
DSS-REAPER-RETRY-COUNT
7. Max number of retries is controlled by DSS_MAX_RETRY_COUNT (10) 

#### Enabling DLQ-based retries for DSS daemons Lambdas

In order to enable DLQ-based reprocessing for DSS daemons each daemon needs to be configured individually. 
Note that currently only 1 daemon dss-sfn-launcher has been configured to enable reprocessing of failed calls in order
to address the issues with Step functions throttling (StartExecution API call).

Locate config.json file in the daemon's .chalice directory
Add the following entry to the config.json file **"dead_letter_queue_target_arn": ""**. 
The entry needs to be created at the top level of the json attribute hierarchy. During deployment the value would be 
replaced with approriate SQS queue name. 

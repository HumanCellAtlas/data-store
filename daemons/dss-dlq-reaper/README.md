# HCA DSS: Dead-letter queue based retry framework

This daemon is a part of the DLQ-based framework for reprocessing failed Lambda invocations

#### About the DLQ retry framework

1. The retry framework is based on the DLQ support built into AWS Lambda. When Lambda invocation fails the message that triggered the invocation is placed into DLQ
2. The daemon is triggred by cloudwatch cron every minute
3. The daemon retrieves messages from SQS queue  `dss-dlq-{stage}`
4. The SNS messages are resent to the original SNS topic ARN with original payload
6. The SNS message attribute DSS-REAPER-RETRY-COUNT tracks the number of reprocess attempts on the given message. When this exceeds DSS_MAX_RETRY_COUNT (default 10), the daemon gives up and removes the message from the queue without retrying.

#### Enabling DLQ-based retries for DSS daemons Lambdas

In order to enable DLQ-based reprocessing for DSS daemons each daemon needs to be configured individually. 

- Locate config.json file in the daemon's .chalice directory
- Add the following entry to the `config.json` file `"dead_letter_queue_target_arn": "",`. 
- The entry needs to be created at the top level of the json attribute hierarchy. During deployment the value would be replaced with approriate SQS queue name. 

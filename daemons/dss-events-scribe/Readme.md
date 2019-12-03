# DSS Event Journaling and Update Daemon

The dss-events-scribe daemon compiles event data into journals, and applies event updates and deletes to existing
journals.

DSS events are managed with the [flashflood library](https://github.com/HumanCellAtlas/flash-flood).

## Concurrency

The flashflood event journaling and update API is not concurrency safe, so this daemon uses a synchronous
execution model by setting Lambda reserved concurrency to 1. For more information about Lambda reserved concurrency,
see [AWS documentation](https://docs.aws.amazon.com/lambda/latest/dg/per-function-concurrency.html).

## Rate limiting

Daemon invication is rate limited similarly to the [token bucket algorithm](https://en.wikipedia.org/wiki/Token_bucket) as follows:
  1) A message is added to a queue every N minutes.
  2) Messages older than `M>N` minutes are discarded from the queue.
  3) The event daemon is invoked non-concurrently on each message until the queue is empty.
If messages cannot be processed quickly enough, the queue will grow to a maximum length of `M/N` items.

`M` and `N` should be adjusted so the daemon does not constantly operate with a full queue.

This algorithm is built on top of
[scheduled AWS CloudWatch rules](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/Create-CloudWatch-Events-Scheduled-Rule.html)
and [AWS SQS queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html).

## Configuration

A scheduled CloudWatch rule is configureed to send one message per replica to the dss-events-scribe SQS queue. The
dss-events-scribe lambda is configured to process messages from the queue in batches of size 1.

- The scheduled CloudWatch rule and SQS queue are configured in [infra/dss-events-scribe/main.tf](../../infra/dss-events-scribe/main.tf).
- Reserved concurrency is configured in [daemons/dss-events-scribe/.chalice/config.json](.chalice/config.json).
- CloudWatch-SQS integration requires an IAM policy on the SQS queue, managed in [infra/dss-events-scribe/main.tf](../../infra/dss-events-scribe/main.tf).
  These permissions should be a superset of SQS permissions assigned to the Lambda execution role in
  [iam/policy-templates/dss-events-scribe-lambda.json](../../iam/policy-templates/dss-events-scribe-lambda.json).
- The queue configurations in [daemons/dss-events-scribe/app.py](app.py) should match the values in 
  [infra/dss-events-scribe/main.tf](../../infra/dss-events-scribe/main.tf), otherwise Domovoi will change the queue configuration upon deploy.


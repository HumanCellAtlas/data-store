# HCA DSS: Scalability Testing framework

This is a scalability test framework for the replicated data storage system (aka the "blue box") of
the [Human Cell Atlas](https://www.humancellatlas.org/).

#### About the scalability testing framework

1. The scalability test framework is based on AWS Step Functions. Workflow definition resembles smoke test for DSS.
1. The execution is triggered by sending SNS messages to `dss-scalability-test-run-{STAGE}` topic
1. The scalability test writes results of execution of individual executions and aggregated run metrics into the
   following DynamoDB tables: `scalability_test_result`, `scalability_test`
1. The SFN execution is initiated and starts by entering WAIT step. Wait is configured to end at the 5 minute intervals
   to accommodate the AWS limit on starting SFN executions and enable generation of bursts of load
1. Once all parallel branches of execution are done, it writes summary of the run in DynamoDB
1. DynamoDB is configured to stream new records into Lambda which aggregates the results and writes incremental metrics
   back into DynamoDB
1. CloudWatch dashboard has been configured to display relevant execution metrics and deployed automatically. The
   dashboard is named as `Scalability{stage}`

#### Running the scale test locally

* Run with default configuration `make scaletest` in the top-level `data-store` directory.
* Run with custom configuration `./tests/scalability/scale_test_runner.py -r <rps> -d <duration_sec>` in the
  top-level `data-store` directory, where `<rps>` is a number of requests generated per second and 
  `<duration_sec>` is the duration of the test in seconds.

#### Adding new tests

New tests can easily be addeed to the existing step function definition at `app.py`.

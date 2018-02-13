# HCA DSS: Scalability Testing framework

This is a scalability test framework for the replicated data storage system (aka the "blue box") of
the [Human Cell Atlas](https://www.humancellatlas.org/). 

#### About the scalability testing framework

1. The scalability test frameworks is based on AWS Step Function. Workflow definition resembles smoke test for DSS.
2. The execution is triggered by sending SNS messages to dss-scalability-test-run-[STAGE} topic
3. The scalability test writes results of execution of individual executions and aggregated run metrics into the 
following DynamoDB tables: scalability_test_run, scalability_test
4. The SFN execution is initiatied and starts by entering WAIT step. Wait is configured to end at the 5 minute intervals 
to accomodate the AWS limit on starting SFN executions and enable generation of bursts of load
5.  Once all parallel branches of execution are done, it writes summary of the run in DynamoDB
6. DynamoDB is configured to stream new records into Lambda which aggregates the results and writes incremental metrics
back into SynamoDB
7. CloudWatch dashboard has been configured to display relevant execution metrics 

#### Running the scale test locally

* Run with default configuration `make scaletest` in the top-level `data-store` directory.
* Run with custom configuration './tests/scalability/scale_test_runner.py -r <rps> -d <seconds>' in the 
top-level `data-store` directory.

#### Adding new tests

New tests can easily be addeed to the existing step function definition at app.py

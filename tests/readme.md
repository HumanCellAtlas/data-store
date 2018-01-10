# Test Convention

All tests are decorated with either `testmode.standalone` or `testmode.integration`. The environment variable 
`DSS_TEST_MODE` selects which type of tests to run. If the word "integration" is in `DSS_TEST_MODE`, then 
integration tests will run. If the word "standalone" is in `DSS_TEST_MODE`, then standalone tests are run. `DSS_TEST_MODE` 
can contain both words, in which case, both sets of tests are run. 

Standalone tests may only use the fixture and storage buckets in each replica. They may not use any other cloud 
resources such as Elasticsearch instances, API gateway or Lambda functions. 

Integration tests require cloud resource to run.

# How to Run

* `make test` will run "standalone" test cases.

* `make integration_test` will run "integration" test cases.

* `make all_tests` will run "standalone" and "integration" tests. 

* `make smoketest` will run the test_smoketest.py test suite.
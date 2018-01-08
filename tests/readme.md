# Test Convention

All test are decorated with either `testmode.standalone` or `testmode.integration`. The environment `DSS_TEST_MODE` selects which type of tests to run. If the word "integration" is in the env var, then it runs the integration tests. If the word "standalone" is in the env var, then it runs the standalone tests. `DSS_TEST_MODE` can contain both words, in which case, both sets of tests are run. 

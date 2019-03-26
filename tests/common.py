import functools
import json
import random
import string
import time
import logging


from fusillade.clouddirectory import publish_schema, create_directory, cleanup_directory, CloudDirectory

random.seed(time.time())

logger = logging.getLogger()
schema_name = 'authz'


def random_hex_string(length=8):
    return ''.join([random.choice(string.hexdigits) for i in range(length)])


def new_test_directory(directory_name=None):
    directory_name = directory_name if directory_name else "test_dir_" + random_hex_string()
    schema_arn = publish_schema(schema_name, 'T' + random_hex_string())
    directory = create_directory(directory_name, schema_arn)
    return directory, schema_arn


def create_test_statement(name: str):
    """Assists with the creation of policy statements for testing"""
    statement = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DefaultRole",
                "Effect": "Deny",
                "Action": [
                    "fake:action"
                ],
                "Resource": "fake:resource"
            }
        ]
    }
    statement["Statement"][0]["Sid"] = name
    return json.dumps(statement)


def eventually(timeout: float, interval: float, errors: set = {AssertionError}):
    """
    @eventually runs a test until all assertions are satisfied or a timeout is reached.
    :param timeout: time until the test fails
    :param interval: time between attempts of the test
    :param errors: the exceptions to catch and retry on
    :return: the result of the function or a raised assertion error
    """
    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
            timeout_time = time.time() + timeout
            error_tuple = tuple(errors)
            while True:
                try:
                    return func(*args, **kwargs)
                except error_tuple as e:
                    if time.time() >= timeout_time:
                        raise
                    logger.debug("Error in %s: %s. Retrying after %s s...", func, e, interval)
                    time.sleep(interval)

        return call

    return decorate

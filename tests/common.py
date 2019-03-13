import functools
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


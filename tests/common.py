import random
import string
import time
import logging


random.seed(time.time())

logger = logging.getLogger()


def random_hex_string(length=8):
    return ''.join([random.choice(string.hexdigits) for i in range(length)])

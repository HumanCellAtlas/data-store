import logging
from logging import DEBUG, INFO, WARNING
import os
import sys
from typing import Mapping, Union, Tuple

import dss
from dss.config import Config

# A type alias for log level maps
log_level_t = Mapping[Union[None, str, logging.Logger], Tuple[int, ...]]

main_log_levels: log_level_t = {
    None: (WARNING, INFO, DEBUG),
    dss.logger: (INFO, DEBUG),
    'app': (INFO, DEBUG),
    'botocore.vendored.requests.packages.urllib3.connectionpool': (WARNING, WARNING, DEBUG)
}
"""
The main log level map. Each entry in the map configures a logger and its children. The parent child relationship
between loggers is established solely by a naming convention. The logger "foo" is a parent of the logger "foo.bar".
We use the module name as the logger name so a child module's logger is a child of the parent module's logger. In
other words, the logger hierarchy follows the module hierarchy. Some framework specific loggers buck that convention
and you are discouraged from using them.

The keys in this map can be None, strings or logger instances. If a key is a string, then it is used to look up the
logger instance by name. A key of None refers to the root logger.

The values in this map are tuples of log levels. The first (second or third) tuple element is used if DSS_DEBUG is 0
(1 or 2). If DSS_DEBUG indexes a non-existing tuple element, the last tuple element is used instead.
"""

test_log_levels: log_level_t = {
    dss.logger: (WARNING, DEBUG),
    'test.es': (INFO, DEBUG)
}
"""
The log levels for running tests. The entries in this map override or extend the entries in the main map.
"""


def configure_cli_logging():
    """
    Prepare logging for use in a command line application.
    """
    _configure_logging(stream=sys.stderr)


def configure_daemon_logging():
    """
    Prepare logging for use within a AWS Lambda function.
    """
    _configure_logging(stream=sys.stdout)


def configure_test_logging():
    """
    Configure logging for use during unit tests.
    """
    _configure_logging(stream=sys.stderr, test=True)


def _configure_logging(test=False, **kwargs):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    if 'AWS_LAMBDA_LOG_GROUP_NAME' in os.environ:
        pass  # On AWS Lambda, we assume that its runtime already configured logging as appropriate
    elif len(root_logger.handlers) == 0:
        logging.basicConfig(**kwargs)
    else:
        root_logger.warning("It appears that logging was already configured in this interpreter process. The currently "
                            "registered handlers, formatters and filters will be left as is.", stack_info=True)
    debug = Config.debug_level()
    log_levels = main_log_levels
    if test:
        log_levels = {**log_levels, **test_log_levels}
    for logger, levels in log_levels.items():
        if isinstance(logger, (str, type(None))):
            logger = logging.getLogger(logger)
        level = levels[min(debug, len(levels) - 1)]
        logger.setLevel(level)

import time
from pythonjsonlogger import jsonlogger
import logging
from logging import DEBUG, INFO, WARNING, ERROR
import os
import sys
from typing import Mapping, Union, Tuple, Optional, List

from pythonjsonlogger.jsonlogger import RESERVED_ATTR_HASH, merge_record_extra

import dss
from dss.config import Config

# A type alias for log level maps
from dss.util.tracing import configure_xray_logging

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
    'dss.stepfunctions.visitation.storage': (ERROR, DEBUG),
    'test.es': (INFO, DEBUG)
}
"""
The log levels for running tests. The entries in this map override or extend the entries in the main map.
"""


LOGGED_FIELDS = ['levelname', 'asctime', 'aws_request_id', 'thread', 'message']
LOG_FORMAT = '(' + ')('.join(LOGGED_FIELDS) + ')'  # format required for DSSJsonFormatter
"""
The fields to log using the json logger.
"""


class DSSJsonFormatter(jsonlogger.JsonFormatter):
    default_time_format = '%Y-%m-%dT%H:%M:%S'
    default_msec_format = '%s.%03dZ'

    converter = time.gmtime

    def add_required_fields(self, fields: List[str]):
        self._required_fields += [field for field in fields if field not in self._required_fields]
        self._skip_fields = dict(zip(self._required_fields,
                                     self._required_fields))
        self._skip_fields.update(RESERVED_ATTR_HASH)

    def set_required_fields(self, fields: List[str]):
        self._required_fields = fields
        self._skip_fields = dict(zip(self._required_fields,
                                     self._required_fields))
        self._skip_fields.update(RESERVED_ATTR_HASH)

    def add_fields(self, log_record, record, message_dict):
        for field in self._required_fields:
            value = record.__dict__.get(field)
            if value:
                log_record[field] = value
        log_record.update(message_dict)
        merge_record_extra(record, log_record, reserved=self._skip_fields)


class DispatchFilter(logging.Filter):
    def filter(self, record):
        return False if '[dispatch]' in record.msg else True


def configure_cli_logging():
    """
    Prepare logging for use in a command line application.
    """
    logHandler = logging.StreamHandler(stream=sys.stderr)
    formatter = DSSJsonFormatter()
    logHandler.setFormatter(formatter)
    _configure_logging(handlers=[logHandler])


def configure_lambda_logging():
    """
    Prepare logging for use within a AWS Lambda function.
    """
    logHandler = logging.StreamHandler(stream=sys.stderr)
    formatter = DSSJsonFormatter()
    logHandler.setFormatter(formatter)
    _configure_logging(handlers=[logHandler])


def configure_test_logging(log_levels: Optional[log_level_t] = None, **kwargs):
    """
    Configure logging for use during unit tests.
    """
    logHandler = logging.StreamHandler(stream=sys.stderr)
    formatter = DSSJsonFormatter()
    logHandler.setFormatter(formatter)
    _configure_logging(test=True, handlers=[logHandler], log_levels=log_levels, **kwargs)


_logging_configured = False


def _configure_logging(test=False, log_levels: Optional[log_level_t] = None, **kwargs):
    root_logger = logging.getLogger()
    global _logging_configured
    if _logging_configured:
        root_logger.info("Logging was already configured in this interpreter process. The currently "
                         "registered handlers, formatters, filters and log levels will be left as is.")
    else:
        root_logger.setLevel(logging.WARNING)
        if 'AWS_LAMBDA_LOG_GROUP_NAME' in os.environ:
            for handler in root_logger.handlers:
                formatter = DSSJsonFormatter(LOG_FORMAT)
                handler.setFormatter(formatter)
                configure_xray_logging(handler)  # Unless xray is enabled
        elif len(root_logger.handlers) == 0:
            logging.basicConfig(**kwargs)
        else:
            # If this happens, the process can likely proceed but the underlying issue needs to be investigated. Some
            # module isn't playing nicely and configured logging before we had a chance to do so. The backtrace
            # included in the log message may look scary but it should aid in finding the culprit.
            root_logger.warning("It appears that logging was already configured in this interpreter process. "
                                "Currently registered handlers, formatters and filters will be left as is.",
                                stack_info=True)
        debug = Config.debug_level()
        _log_levels = main_log_levels
        if test:
            _log_levels = {**_log_levels, **test_log_levels}
        if log_levels:
            _log_levels = {**_log_levels, **log_levels}
        for logger, levels in _log_levels.items():
            if isinstance(logger, (str, type(None))):
                logger = logging.getLogger(logger)
            level = levels[min(debug, len(levels) - 1)]
            logger.setLevel(level)
        _logging_configured = True

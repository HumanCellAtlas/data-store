import logging
import os
import sys
import time
from logging import LogRecord
from typing import List

from pythonjsonlogger import jsonlogger
from pythonjsonlogger.jsonlogger import merge_record_extra

from fusillade import Config

LOGGED_FIELDS = ['levelname', 'asctime', 'aws_request_id', 'name', 'message', 'thread']
LOG_FORMAT = f"({')('.join(LOGGED_FIELDS)})"  # format required for DSSJsonFormatter

"""
The fields to log using the json logger.
"""


class FUSJsonFormatter(jsonlogger.JsonFormatter):
    default_time_format = '%Y-%m-%dT%H:%M:%S'
    default_msec_format = '%s.%03dZ'

    converter = time.gmtime

    def add_required_fields(self, fields: List[str]) -> None:
        """
        Add additional required fields to to be written in log messages. New fields will be added to the end of the
        `required_fields` list in the order specified by `fields`.

        :param fields: an ordered list of required fields to write to logs.
        :return:
        """

        self._required_fields += [field for field in fields if field not in self._required_fields]
        self._skip_fields = dict(zip(self._required_fields,
                                     self._required_fields))
        # self._skip_fields.update(RESERVED_ATTR_HASH)

    def set_required_fields(self, fields: List[str]) -> None:
        """
        Sets the required fields in the order specified in `fields`. Required fields appears in the logs in the order
        listed in `required_fields`.

        :param fields: an ordered list of fields to set `required_fields`
        :return:
        """
        self._required_fields = fields
        self._skip_fields = dict(zip(self._required_fields,
                                     self._required_fields))
        # self._skip_fields.update(RESERVED_ATTR_HASH)

    def add_fields(self, log_record: dict, record: LogRecord, message_dict: dict) -> None:
        """
        Adds additional log information from `log_record` to `records. If a required field does not exist in the
        `log_record` then it is not included in the `record`.

        :param log_record: additional fields to add to the `record`.
        :param record: the logRecord to add additional fields too.
        :param message_dict: the log message and extra fields to add to `records`.
        :return:
        """
        for field in self._required_fields:
            value = record.__dict__.get(field)
            if value:
                log_record[field] = value
        log_record.update(message_dict)
        merge_record_extra(record, log_record, reserved=self._skip_fields)


def _get_json_log_handler():
    log_handler = logging.StreamHandler(stream=sys.stderr)
    log_handler.setFormatter(FUSJsonFormatter(LOG_FORMAT))
    return log_handler


def configure_lambda_logging():
    """
    Prepare logging for use within a AWS Lambda function.
    """
    if _json_logs:
        _configure_logging(handlers=[_get_json_log_handler()])
    else:
        _configure_logging()


"""
Subclasses Chalice to configure all Python loggers to our liking.
"""
silence_debug_loggers = ["botocore"]
_logging_configured = False
_debug = True
_json_logs = bool(os.getenv('JSON_LOGS', False))


def _configure_logging(**kwargs):
    root_logger = logging.root
    global _logging_configured, _debug, silence_debug_loggers, _json_logs
    if _logging_configured:
        root_logger.info("Logging was already configured in this interpreter process. The currently "
                         "registered handlers, formatters, filters and log levels will be left as is.")
    else:
        root_logger.setLevel(logging.WARNING)
        if len(root_logger.handlers) == 0:
            logging.basicConfig(**kwargs)
        else:
            # If this happens, the process can likely proceed but the underlying issue needs to be investigated. Some
            # module isn't playing nicely and configured logging before we had a chance to do so. The backtrace
            # included in the log message may look scary but it should aid in finding the culprit.
            root_logger.warning("It appears that logging was already configured in this interpreter process. "
                                "Currently registered handlers, formatters and filters will be left as is.",
                                stack_info=True)
        if _json_logs:
            for handler in root_logger.handlers:
                formatter = FUSJsonFormatter(LOG_FORMAT)
                handler.setFormatter(formatter)
        if Config.log_level() == 0:
            _debug = False
            root_logger.setLevel(logging.WARN)
        elif Config.log_level() == 1:
            root_logger.setLevel(logging.INFO)
        elif Config.log_level() > 1:
            root_logger.setLevel(logging.DEBUG)
            for logger_name in silence_debug_loggers:
                logging.getLogger(logger_name).setLevel(logging.INFO)
        _logging_configured = True


def is_debug() -> bool:
    global _debug
    return _debug

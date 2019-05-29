import os
import time
from pythonjsonlogger import jsonlogger
import logging
from logging import LogRecord
import sys
from typing import List

from pythonjsonlogger.jsonlogger import merge_record_extra

from fusillade import Config

LOGGED_FIELDS = ['levelname', 'asctime', 'aws_request_id', 'name', 'message', 'thread']
LOG_FORMAT = f"({')('.join(LOGGED_FIELDS)})"  # format required for DSSJsonFormatter

"""
The fields to log using the json logger.
"""


class DSSJsonFormatter(jsonlogger.JsonFormatter):
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
    log_handler.setFormatter(DSSJsonFormatter())
    return log_handler


def configure_cli_logging():
    """
    Prepare logging for use in a command line application.
    """
    _configure_logging(handlers=[_get_json_log_handler()])


def configure_lambda_logging():
    """
    Prepare logging for use within a AWS Lambda function.
    """
    _configure_logging(handlers=[_get_json_log_handler()])


def configure_test_logging(**kwargs):
    """
    Configure logging for use during unit tests.
    """
    _configure_logging(test=True,
                       handlers=[_get_json_log_handler()],
                       **kwargs)


"""
Subclasses Chalice to configure all Python loggers to our liking.
"""
silence_debug_loggers = ["botocore"]
_logging_configured = False
_debug = True


def _configure_logging(**kwargs):
    root_logger = logging.root
    global _logging_configured, _debug, silence_debug_loggers
    logging.basicConfig()

    if bool(os.getenv('JSON_LOGS', False)):
        for handler in root_logger.handlers:
            formatter = DSSJsonFormatter(LOG_FORMAT)
            handler.setFormatter(formatter)

    if _logging_configured:
        root_logger.info("Logging was already configured in this interpreter process. The currently "
                         "registered handlers, formatters, filters and log levels will be left as is.")
    else:
        if len(root_logger.handlers) == 0:
            logging.basicConfig(**kwargs)
        else:
            if bool(os.getenv('JSON_LOGS', False)):
                for handler in root_logger.handlers:
                    formatter = DSSJsonFormatter(LOG_FORMAT)
                    handler.setFormatter(formatter)
        if Config.debug_level() == 0:
            _debug = False
            root_logger.setLevel(logging.WARN)
        elif Config.debug_level() == 1:
            root_logger.setLevel(logging.INFO)
        elif Config.debug_level() > 1:
            root_logger.setLevel(logging.DEBUG)
            for logger_name in silence_debug_loggers:
                logging.getLogger(logger_name).setLevel(logging.INFO)


def is_debug() -> bool:
    global _debug
    return _debug

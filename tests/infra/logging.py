import logging


def start_verbose_logging():
    logging.basicConfig(level=logging.INFO)
    for logger_name in logging.Logger.manager.loggerDict:  # type: ignore
        if (logger_name.startswith("botocore") or
                logger_name.startswith("boto3.resources") or
                logger_name.startswith("elasticsearch") or
                logger_name.startswith("org.elasticsearch")):
            logging.getLogger(logger_name).setLevel(logging.WARNING)

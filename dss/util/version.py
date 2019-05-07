import datetime


def datetime_to_version_format(timestamp: datetime.datetime) -> str:
    return timestamp.strftime("%Y-%m-%dT%H%M%S.%fZ")

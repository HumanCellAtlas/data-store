import datetime

_datetime_format = "%Y-%m-%dT%H%M%S.%fZ"

def datetime_to_version_format(timestamp: datetime.datetime) -> str:
    return timestamp.strftime(_datetime_format)

def datetime_from_timestamp(ts: str) -> datetime.datetime:
    return datetime.datetime.strptime(ts, _datetime_format)

import datetime


def get_version():
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")

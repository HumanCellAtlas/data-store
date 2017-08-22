import json
import botocore
from . import resources, clients


AWS_MIN_CHUNK_SIZE = 64 * 1024 * 1024
"""Files must be larger than this before we consider multipart uploads."""
AWS_MAX_MULTIPART_COUNT = 10000
"""Maximum number of parts allowed in a multipart upload.  This is a limitation imposed by S3."""

class ARN:
    fields = "arn partition service region account_id resource".split()
    _default_region, _default_account_id, _default_iam_username = None, None, None
    def __init__(self, arn="arn:aws::::", **kwargs):
        self.__dict__.update(dict(zip(self.fields, arn.split(":", 5)), **kwargs))
        if "region" not in kwargs and not self.region:
            self.region = self.get_region()
        if "account_id" not in kwargs and not self.account_id:
            self.account_id = self.get_account_id()

    @classmethod
    def get_region(cls):
        if cls._default_region is None:
            cls._default_region = botocore.session.Session().get_config_variable("region")
        return cls._default_region

    @classmethod
    def get_account_id(cls):
        if cls._default_account_id is None:
            cls._default_account_id = clients.sts.get_caller_identity()["Account"]
        return cls._default_account_id

    def __str__(self):
        return ":".join(getattr(self, field) for field in self.fields)


def send_sns_msg(topic_arn, message):
    sns_topic = resources.sns.Topic(str(topic_arn))
    sns_topic.publish(Message=json.dumps(message))


def get_s3_chunk_size(filesize: int) -> int:
    if filesize <= AWS_MAX_MULTIPART_COUNT * AWS_MIN_CHUNK_SIZE:
        return AWS_MIN_CHUNK_SIZE
    else:
        div = filesize // AWS_MAX_MULTIPART_COUNT
        if div * AWS_MAX_MULTIPART_COUNT < filesize:
            div += 1
        return ((div + 1048575) // 1048576) * 1048576

from typing import Union, Mapping, Any, List

JSONObj = Mapping[str, Any]

# Strictly speaking, this is the generic JSON type:

AnyJSON = Union[str, int, float, bool, None, JSONObj, List[Any]]

# Most JSON structures, however, start with an JSON object, so we'll use the shorter name for that type:

JSON = Mapping[str, AnyJSON]


# A stub for the AWS Lambda context

class LambdaContext(object):

    @property
    def aws_request_id(self) -> str:
        raise NotImplementedError

    @property
    def log_group_name(self) -> str:
        raise NotImplementedError

    @property
    def log_stream_name(self) -> str:
        raise NotImplementedError

    @property
    def function_name(self) -> str:
        raise NotImplementedError

    @property
    def memory_limit_in_mb(self) -> str:
        raise NotImplementedError

    @property
    def function_version(self) -> str:
        raise NotImplementedError

    @property
    def invoked_function_arn(self) -> str:
        raise NotImplementedError

    def get_remaining_time_in_millis(self) -> int:
        raise NotImplementedError

    def log(self, msg: str) -> None:
        raise NotImplementedError

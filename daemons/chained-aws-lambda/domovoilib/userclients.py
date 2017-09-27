import typing

from chainedawslambda import Task

from dss import chained_lambda_clients


def clients() -> typing.Iterable[typing.Tuple[str, typing.Type[Task]]]:
    return chained_lambda_clients()

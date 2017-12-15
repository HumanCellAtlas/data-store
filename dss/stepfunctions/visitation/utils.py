
import time
import typing
import threading
from .. import step_functions_list_executions, step_functions_describe_execution


def current_walker_executions(sentinel_execution_name: str) -> typing.List[str]:
    all_execs = step_functions_list_executions(
        'dss-visitation-{stage}',
        status_filter='RUNNING'
    )

    execs = [e for e in all_execs
             if e['name'].endswith(sentinel_execution_name) and '----' in e['name']]

    return execs


def throttled_iter(iterable: typing.Iterable, calls_per_second: int=2, bunch_size: int=10) -> typing.Iterable:
    t = time.time()
    k = 0

    for item in iterable:

        if k >= bunch_size:
            dt = time.time() - t
            if k / dt > calls_per_second:
                wait_time = k / calls_per_second - dt
                time.sleep(wait_time)

            t = time.time()
            k = 0

        k += 1
        yield item


def walker_execution_name(sentinel_name: str, prefix: str) -> str:
    return '{}----{}'.format(
        prefix.replace('/', '____'),
        sentinel_name
    )


def walker_prefix(walker_execution_name: str) -> str:
    encoded_prefix, sentinel_execution_name = walker_execution_name.split('----')
    prefix = encoded_prefix.replace('____', '/')
    return prefix


class Timeout:
    """
    Decorator.
    Run a method in a background thread with a timeout. The calling thread is blocked.
    Exceptions are captured and raised.
    """
    def __init__(self, seconds: float) -> None:
        self.result: typing.Any = None
        self.exception: Exception = None
        self.timeout_seconds: float = seconds

    def run(self, *args, **kwargs) -> None:
        try:
            self.result = self.func(*args, **kwargs)
        except Exception as ex:
            self.exception = ex

    def start(
        self,
        target: typing.Callable[..., typing.Any],
        args: typing.List[typing.Any],
        kwargs: typing.Dict[str, typing.Any]
    ) -> None:
        self.thread = threading.Thread(target=target, daemon=True, args=args, kwargs=kwargs)
        self.thread.start()
        self.thread.join(self.timeout_seconds)

    def __call__(self, func: typing.Callable[..., typing.Any]) -> typing.Any:
        def wrapped(*args, **kwargs):
            self.func = func
            self.start(self.run, args, kwargs)
            if self.exception is not None:
                raise self.exception
            return self.result
        return wrapped

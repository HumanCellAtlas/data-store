
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

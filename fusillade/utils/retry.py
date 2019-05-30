"""
Copied from https://github.com/HumanCellAtlas/data-store/blob/master/dss/util/retry.py
commit hash: 6b8c2d6767d55d3e6d45bd75e107d882427854e1
"""

import logging
import threading

import itertools

import time
from functools import wraps
from typing import Any, Optional, Callable, Union

default_logger = logging.getLogger(__name__)


# noinspection PyPep8Naming
class retry:
    """
    A function decorator that retries invocations of the decorated function on every unhandled exception. Each
    invocation attempt (the first attempt and each retry) represents a transaction. Contextual information that
    accumulates (via the static :py:meth:`retry.add_context`) over the course of the transaction is logged when an
    exception occurs, the context is reset and the transaction retried.

    @retry without arguments retries the decorated function forever (!), on any exception, without delay.

    >>> default_logger.setLevel(logging.ERROR) # avoid polluting test output with expected warnings

    >>> @retry
    ... def f1(x):
    ...     retry.add_context(x=x)
    ...     import os
    ...     y = ord(os.urandom(1)) % 16
    ...     retry.add_context(y=y)
    ...     if y == x % 16:
    ...         return x
    ...     else:
    ...         raise Exception
    >>> f1(3)
    3

    Note that the `retry.add_context` method can be used anywhere, directly in the decorated function or any function
    it calls.

    In most cases you will want to specify either a maximum number of retries …

    >>> @retry(limit=1)
    ... def f2(x):
    ...     retry.add_context(x=x)
    ...     raise Exception(x)
    >>> f2(42)
    Traceback (most recent call last):
      ...
    Exception: 42

    … or a timeout in seconds. A delay in seconds between retries can also be specified:

    >>> @retry(timeout=0.5, delay=0.1)
    ... def f3(x):
    ...     retry.add_context(x=x)
    ...     raise Exception(x)
    >>> f3(42)
    Traceback (most recent call last):
      ...
    Exception: 42

    A predicate can be used to retry a function only when specific types of exceptions occur:

    >>> i = 0
    >>> @retry(retryable=lambda e: isinstance(e, RuntimeError), limit=2)
    ... def f4(x):
    ...     global i
    ...     i += 1
    ...     raise ValueError(x)
    >>> f4(42)
    Traceback (most recent call last):
      ...
    ValueError: 42
    >>> i
    1

    By default the name of the decorated function is logged when an exception occurs. Both name and logger can be
    configured:

    >>> from unittest.mock import MagicMock
    >>> logger = MagicMock()

    >>> @retry(name='foo', logger=logger, limit=0)
    ... def f5():
    ...     raise Exception
    >>> f5()
    Traceback (most recent call last):
      ...
    Exception
    >>> logger.method_calls[1]
    call.warning('An exception occurred in %r.', retry.TX(name='foo', {}), exc_info=True, stack_info=True)

    If one decorated function calls another decorated function, the inner function will be retried independently and
    each inner TX will commence with an empty context and its own timeout and logger. While this seems desirable it
    could violate a timeout constraint specified for the outer function. Use `inherit=True` to have the inner TX
    inherit the outer TX's context, logger and timeout. Note that while an inner TX inherits the context of the outer
    TX, changes made to that context are only reflected for the duration of the inner TX. Every inner TX starts with
    a fresh copy of the outer TX's original context.

    >>> from unittest.mock import MagicMock
    >>> logger = MagicMock()

    >>> @retry(timeout=0, logger=logger)
    ... def f6(x):
    ...     retry.add_context(x=x)
    ...     return f7(x+x)

    >>> @retry(inherit=True)
    ... def f7(y):
    ...     retry.add_context(y=y)
    ...     raise Exception

    Without `inherit=True`, f7 would be retried forever, the logged context would lack 'x' and logs would go to the
    default logger. With it, the retry of f7 honours the timeout on f6, which is 0 in this case, disabling a retry.
    The log messages emitted for `f7` include `x` from `f6` but those emitted for f6 don't include `y` from f6.

    >>> f6(42)
    Traceback (most recent call last):
      ...
    Exception

    >>> logger.method_calls # doctest: +NORMALIZE_WHITESPACE
    [call.isEnabledFor(30),
     call.warning('An exception occurred in %r.',
                  retry.TX(name='f7', {'x': 42, 'y': 84}), exc_info=True, stack_info=True),
     call.warning('Timed out retrying %r.',
                  retry.TX(name='f7', {'x': 42, 'y': 84})),
     call.isEnabledFor(30),
     call.warning("Exception '%s' occurred in %r. The back trace was logged earlier.",
                  Exception(), retry.TX(name='f6', {'x': 42})),
     call.warning('Timed out retrying %r.', retry.TX(name='f6', {'x': 42}))]

    Note that any retry `limit` constraints are applied independently, regardless of `inherit`. Barring timeouts,
    the lowest upper bound on the number of times an inner function is retried is the product of the inner and outer
    `limit`.

    The delay before each retry can be computed dynamically based on the previous delay and the index of the retry.
    The index of the first retry is 0 and its previous delay is None. The following example implements exponential
    back-off:

    >>> i, t = 0, time.time()
    >>> @retry(delay=lambda _, delay: 0.1 if delay is None else delay * 2, limit=4)
    ... def f8():
    ...     global i; i += 1; raise Exception
    >>> f8()
    Traceback (most recent call last):
      ...
    Exception
    >>> i
    5
    >>> time.time() - t >= 1.5
    True

    To use a fixed list of delays:

    >>> i, t = 0, time.time()
    >>> delays = [0.1, 0.2, 0.4, 0.8]
    >>> @retry(delay=lambda j, _: delays[j], limit=len(delays))
    ... def f9():
    ...     global i; i += 1; raise Exception
    >>> f9()
    Traceback (most recent call last):
      ...
    Exception
    >>> i
    5
    >>> time.time() - t >= 1.5
    True
    """

    thread_local = threading.local()

    def __init__(self,
                 inherit: bool = False,
                 name: Optional[str] = None,
                 limit: Optional[int] = None,
                 timeout: Optional[float] = None,
                 retryable: Callable[[BaseException], bool] = lambda e: True,
                 delay: Union[float, Callable[[int, float], float]] = 0,
                 logger=None) -> None:
        super().__init__()
        self.inherit = inherit
        self.name = name
        self.limit = limit
        self.timeout = timeout
        self.retryable = retryable
        self.delay = delay if callable(delay) else lambda *_: delay
        self.logger = logger or default_logger

    def __call__(self, f):
        name = self.name or f.__name__

        @wraps(f)
        def wrapper(*args, **kwargs):
            outer_tx = self._get_tx()
            if not self.inherit or outer_tx is None:
                inner_tx = self._new_tx(name)
            else:
                inner_tx = outer_tx.copy(name)
            delay = None
            attempts = itertools.count() if self.limit is None else iter(range(self.limit))
            while True:
                tx = inner_tx.copy()
                self._set_tx(tx)
                try:
                    return f(*args, **kwargs)
                except BaseException as e:
                    tx.log_exception(e)
                    if self.retryable(e):
                        attempt = next(attempts, None)
                        if attempt is None:
                            tx.logger.warning("Exceeded retry limit for %r.", tx)
                            raise
                        delay = self.delay(attempt, delay)
                        if tx.would_expire_after(delay):
                            tx.logger.warning("Timed out retrying %r.", tx)
                            raise
                        else:
                            tx.logger.debug("Sleeping %f seconds before retrying %r.", delay, tx)
                            time.sleep(delay)
                            tx.logger.info("Retrying %r.", tx)
                    else:
                        raise
                finally:
                    self._set_tx(outer_tx)

        return wrapper

    def _new_tx(self, name: str) -> 'TX':
        timeout = self.timeout
        expiration = None if timeout is None else time.time() + timeout
        return self.TX(name=name, expiration=expiration, logger=self.logger)

    @classmethod
    def _get_tx(cls) -> Optional['TX']:
        return getattr(cls.thread_local, 'tx', None)

    @classmethod
    def _set_tx(cls, tx: 'TX'):
        cls.thread_local.tx = tx

    @classmethod
    def get_tx(cls) -> 'TX':
        tx = cls._get_tx()
        if tx is None:
            raise LookupError('No retry transaction active in current thread')
        else:
            return tx

    @classmethod
    def add_context(cls, **kwargs) -> None:
        cls.get_tx().update(kwargs)

    @classmethod
    def set_logger(cls, logger: logging.Logger) -> None:
        cls.get_tx().logger = logger

    class TX(dict):

        def __init__(self, name: str, expiration: float, logger: logging.Logger) -> None:
            super().__init__()
            self.name = name
            self.logger = logger
            self.expiration = expiration

        def would_expire_after(self, delay: float):
            return self.expiration is not None and self.expiration < time.time() + delay

        def copy(self, name: str = None):
            other = type(self)(name=name or self.name, expiration=self.expiration, logger=self.logger)
            other.update(self)
            return other

        def log_exception(self, e: BaseException) -> None:
            if self.logger.isEnabledFor(logging.WARNING):
                logged_attr_name = f"_{__name__}_already_logged"
                already_logged = getattr(e, logged_attr_name, False)
                if already_logged:
                    self.logger.warning("Exception '%s' occurred in %r. The back trace was logged earlier.", e, self)
                else:
                    self.logger.warning("An exception occurred in %r.", self, exc_info=True, stack_info=True)
                    try:
                        setattr(e, logged_attr_name, True)
                    except AttributeError:
                        self.logger.warning('Failed to mark exception as logged. The back trace may be logged again.')

        def __repr__(self):
            return f"retry.TX(name='{self.name}', {super().__repr__()})"

    # Overide __new__ to make @retry equivalent to @retry()

    def __new__(cls, *args, **kwargs) -> Any:
        if args and callable(args[0]):
            assert len(args) == 1, 'Decorators should only be invoked with a single, callable argument'
            self = super().__new__(cls)
            self.__init__()
            return self(args[0])
        else:
            return super().__new__(cls)

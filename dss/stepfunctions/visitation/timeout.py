import signal
from contextlib import AbstractContextManager

class Timeout(AbstractContextManager):
    def __init__(self, seconds_remaining: int) -> None:
        self.did_timeout = False
        self.seconds_remaining = seconds_remaining

    def __enter__(self):
        def _timeout_handler(signum, frame):
            raise TimeoutError("time's up!")

        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(self.seconds_remaining)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        signal.alarm(0)

        if exc_type == TimeoutError:
            self.did_timeout = True
            return True

        return None is exc_type

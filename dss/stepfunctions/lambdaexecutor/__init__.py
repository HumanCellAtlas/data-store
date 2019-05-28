import copy
import threading
import typing


StateType = typing.TypeVar('StateType')


class TimedThread(typing.Generic[StateType]):
    """
    This is a "Thread" class that runs a job for a maximum period of time.  The class provides concurrency-safe methods
    to retrieve and persist a chunk of state.
    """
    def __init__(self, timeout_seconds: float, state: StateType) -> None:
        self.timeout_seconds = timeout_seconds
        self.__state = copy.deepcopy(state)
        self.lock = threading.Lock()
        self._exception: Exception = None

    def run(self) -> StateType:
        raise NotImplementedError()

    def _run(self) -> None:
        try:
            state = self.run()
        except Exception as e:
            self._exception = e
        else:
            self.save_state(state)

    def _start_async(self) -> None:
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _join(self) -> StateType:
        self.thread.join(self.timeout_seconds)

        with self.lock:
            state = copy.deepcopy(self.__state)
        return state

    def start(self) -> StateType:
        self._start_async()
        state = self._join()
        if self._exception:
            raise self._exception
        return state

    def get_state_copy(self) -> StateType:
        with self.lock:
            state_copy = copy.deepcopy(self.__state)
        return state_copy

    def save_state(self, new_state: StateType) -> None:
        new_state = copy.deepcopy(new_state)
        with self.lock:
            self.__state = new_state

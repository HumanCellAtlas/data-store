#!/usr/bin/env python
# coding: utf-8

import os
import sys
import threading
import time
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss.stepfunctions.lambdaexecutor as lambdaexecutor


class TestLambdaExecutor(unittest.TestCase):
    def test_state_freezing(self):
        """Test that we freeze the state at the time when the timeout expires."""
        class UUT(lambdaexecutor.TimedThread[dict]):
            KEY = "key"

            def __init__(self) -> None:
                super().__init__(5, {UUT.KEY: 0})

            def run(self) -> dict:
                for ix in range(15):
                    state = self.get_state_copy()
                    state[UUT.KEY] += 1
                    self.save_state(state)
                    time.sleep(1)
                return state

        uut = UUT()
        result = uut.start()
        self.assertGreaterEqual(result[UUT.KEY], 4)
        self.assertLessEqual(result[UUT.KEY], 6)

    def test_exit(self):
        """Test that we save the final state that `run()` returns."""
        class UUT(lambdaexecutor.TimedThread[dict]):
            KEY = "key"

            def __init__(self) -> None:
                super().__init__(5, {UUT.KEY: 0})

            def run(self) -> dict:
                state = self.get_state_copy()
                for ix in range(15):
                    state[UUT.KEY] += 1
                return state

        uut = UUT()
        result = uut.start()
        self.assertEqual(result[UUT.KEY], 15)

    def test_immutable_constructor_state(self):
        """Test that we make a copy of the state when we construct a TimedThread."""
        class UUT(lambdaexecutor.TimedThread[dict]):
            KEY = "key"

            def __init__(self, state: dict, event: threading.Event) -> None:
                super().__init__(5, state)
                self.event = event

            def run(uut_self) -> dict:
                uut_self.event.wait()
                state = uut_self.get_state_copy()
                self.assertEqual(state[UUT.KEY], 0)
                return state

        state = {UUT.KEY: 0}
        event = threading.Event()
        uut = UUT(state, event)
        uut._start_async()
        state[UUT.KEY] = 1
        event.set()
        uut._join()

    def test_immutable_get_state(self):
        """Test that we make a copy of the state when we get it."""
        class UUT(lambdaexecutor.TimedThread[dict]):
            KEY = "key"

            def __init__(self) -> None:
                super().__init__(5, {UUT.KEY: 0})

            def run(uut_self) -> dict:
                state = uut_self.get_state_copy()
                state[UUT.KEY] = 1
                self.assertNotEqual(state, uut_self.get_state_copy())
                return state

        uut = UUT()
        result = uut.start()
        self.assertEqual(result[UUT.KEY], 1)

    def test_immutable_set_state(self):
        """Test that we make a copy of the state when we set it."""
        class UUT(lambdaexecutor.TimedThread[dict]):
            KEY = "key"

            def __init__(self) -> None:
                super().__init__(5, {UUT.KEY: 0})

            def run(uut_self) -> dict:
                state = {UUT.KEY: 15}
                uut_self.save_state(state)
                state[UUT.KEY] = 5
                self.assertEqual(uut_self.get_state_copy()[UUT.KEY], 15)
                return state

        uut = UUT()
        result = uut.start()
        self.assertEqual(result[UUT.KEY], 5)


if __name__ == '__main__':
    unittest.main()

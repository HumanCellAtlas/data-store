#!/usr/bin/env python
# coding: utf-8
from collections import Counter
from http.server import HTTPServer, BaseHTTPRequestHandler
from itertools import count, permutations
import json
import logging
from math import sqrt
from socketserver import ThreadingMixIn
from typing import List, Tuple, Optional
from unittest import mock

import random
import threading
import uuid

import os
import sys
import unittest

import time

import requests
from requests_http_signature import HTTPSignatureAuth

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Config
from dss.logging import configure_test_logging
from dss.notify.notification import Notification
from dss.notify.notifier import Notifier
from dss.util import networking
from dss.util.types import LambdaContext

logger = logging.getLogger(__name__)


def setUpModule():
    # Add thread names and timing information to test logs. Also, since the tests intentionally involve numerous
    # exceptions logged at WARNING level, we increase the log level to ERROR. Set DSS_DEBUG to 1 for verbose logs.
    configure_test_logging(format="%(asctime)s %(levelname)s %(name)s %(threadName)s: %(message)s",
                           log_levels={dss.notify.__name__: (logging.ERROR, logging.DEBUG)})


class ThreadedHttpServerTestCase(unittest.TestCase):
    server = None
    port = None
    address = "127.0.0.1"
    server_thread = None
    server_exception = None

    class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
        pass

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.port = networking.unused_tcp_port()
        cls.server = cls.ThreadedHTTPServer((cls.address, cls.port), PostTestHandler)
        cls.server_thread = threading.Thread(target=cls._serve)
        cls.server_thread.start()

    @classmethod
    def _serve(cls):
        try:
            cls.server.serve_forever()
        except BaseException as e:
            cls.server_exception = e

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server_thread.join()
        super().tearDownClass()
        if cls.server_exception:
            raise cls.server_exception


class _TestNotifier(ThreadedHttpServerTestCase):
    repeats = 1
    timeout = 1.0
    overhead = 5.0
    delays = [0.0, 1.0, 2.0]
    workers_per_queue: Optional[int] = None

    @property
    def num_queues(self):
        return len(self.delays)

    @property
    def num_workers(self):
        return self.workers_per_queue * self.num_queues

    def setUp(self):
        logger.critical("Random state for test reproduction: %r", random.getstate())
        self.assertFalse(self.workers_per_queue is None, "Concrete subclasses must define workers_per_queue")
        self.notification_id = count()
        self.subscription_ids = [f"{self.__class__.__name__}-{self._testMethodName}-{time.time()}-{i}"
                                 for i in range(2)]
        PostTestHandler.reset()
        self.notifier = Notifier(deployment_stage=f'test-{uuid.uuid4()}',
                                 delays=self.delays,
                                 num_workers=self.num_workers,
                                 sqs_polling_timeout=5,
                                 timeout=self.timeout,
                                 overhead=self.overhead)
        self.notifier.deploy()

    def _mock_context(self, total_attempts):
        latency = sum(delay for delay in self.delays)
        average_overhead = self.overhead / 3  # FIXME: this is a guess
        average_timeout = self.timeout / 2  # FIXME: this is a guess
        parallelism = self.num_queues + (self.num_workers - self.num_queues) / 5  # FIXME: this is a guess
        total_time = latency + (average_timeout + average_overhead) * total_attempts / parallelism
        return MockLambdaContext(total_time)

    def tearDown(self):
        self.notifier.destroy()

    def test(self):
        expected_receptions = set()
        expected_misses = set()
        total_attempts = 0

        def notify(expect: bool,  # whether the message should make it
                   max_attempts: Optional[int] = None,  # how many attempts to allow
                   responses: List[Tuple[float, int]] = None,  # a list of (delay, http_status) tuples, one per attempt
                   attempts=None):  # expected number of attempts, currently only used to estmate the running time
            if responses is None:
                responses = [(0.0, 200)]
            verify = random.random() > .5
            notification_id = str(next(self.notification_id))
            payload = dict(notification_id=notification_id,
                           responses=responses,
                           verify=verify)
            notification = Notification.from_scratch(notification_id=notification_id,
                                                     subscription_id=str(random.choice(self.subscription_ids)),
                                                     url=f"http://{self.address}:{self.port}/{notification_id}",
                                                     payload=payload,
                                                     attempts=max_attempts,
                                                     hmac_key=PostTestHandler.hmac_secret_key if verify else None,
                                                     hmac_key_id='1234' if verify else None)
            nonlocal total_attempts
            total_attempts += min(notification.attempts, self.num_queues) if attempts is None else attempts
            (expected_receptions if expect else expected_misses).add(notification_id)
            self.notifier.enqueue(notification)

        for repeat in range(self.repeats):
            # A notification with …
            # … zero permitted attempts will be missed.
            notify(responses=[], max_attempts=0, expect=False)
            for n in range(1, self.num_queues + 1):
                # … n attempts makes it if it succeeds the n-th time.
                notify(responses=[(0, 500)] * (n - 1) + [(0, 200)], max_attempts=n, expect=True)
                # … n attempts is missed even if it would otherwise succeed the n+1-th time.
                notify(responses=[(0, 500)] * n + [(0, 200)], max_attempts=n, expect=False)
            # A notification whose endpoint …
            # … consistently that takes too long to respond, will be missed.
            notify(responses=[(self.timeout * 2, 200)], expect=False)
            # … takes too long to deliver only once will make it the second time
            notify(responses=[(self.timeout * 2, 200), (0, 200)], expect=True, attempts=2)
            # … takes a little longer to deliver will make it the first time
            notify(responses=[(self.timeout / 2, 200)], expect=True, attempts=1)
            # … takes a little longer to deliver, but then fails, will be missed
            notify(responses=[(self.timeout / 2, 500)], expect=False)

            self.notifier.run(self._mock_context(total_attempts))

        actual_receptions = set(PostTestHandler.actual_receptions)
        self.assertEqual(expected_receptions, actual_receptions)
        self.assertEqual(set(), actual_receptions.intersection(expected_misses))
        self.assertEqual(len(actual_receptions), len(PostTestHandler.actual_receptions))


class TestNotifierOne(_TestNotifier):
    workers_per_queue = 1


class TestNotifierTwo(_TestNotifier):
    workers_per_queue = 2


del _TestNotifier


class TestNotifierConfig(unittest.TestCase):

    def test_notifier_from_config(self):
        with mock.patch.dict(os.environ,
                             DSS_NOTIFY_DELAYS="",
                             DSS_NOTIFY_WORKERS="",
                             DSS_NOTIFY_ATTEMPTS=""):
            self.assertFalse(Config.notification_is_async())
            self.assertEqual(Config.notification_attempts(), 0)
            self.assertEqual(Config.notification_delays(), [])
            with mock.patch.dict(os.environ, DSS_NOTIFY_DELAYS="0"):
                self.assertTrue(Config.notification_is_async())
                self.assertEqual(Config.notification_attempts(), 1)
                self.assertEqual(Config.notification_delays(), [0])
                with mock.patch.dict(os.environ, DSS_NOTIFY_ATTEMPTS="0"):
                    self.assertEqual(Config.notification_attempts(), 0)
                    self.assertFalse(Config.notification_is_async())

        with mock.patch.dict(os.environ,
                             DSS_NOTIFY_DELAYS="3 2 1 .5",
                             DSS_NOTIFY_WORKERS="7"):
            notifier = Notifier.from_config()
            self.assertEqual([3.0, 2.0, 1.0, .5], notifier._delays)
            self.assertEqual(7, notifier._num_workers)
            self.assertTrue(Config.notification_is_async())
            self.assertEqual(Config.notification_attempts(), 4)


class TestWorkerQueueAssignment(unittest.TestCase):
    def _test(self, num_workers, queue_lengths):
        notifier = Notifier(deployment_stage='foo', delays=[0] * len(queue_lengths), num_workers=num_workers)
        return list(notifier._work_queue_indices(queue_lengths))

    def test_edge_cases(self):
        self.assertEquals(self._test(num_workers=1, queue_lengths=[0]), [0])
        self.assertEquals(self._test(num_workers=2, queue_lengths=[0]), [0, 0])
        self.assertIn(self._test(num_workers=1, queue_lengths=[0, 0]), ([0], [1]))

    def test_worker_surplus(self):
        num_workers = 10
        repeats = 1000
        imbalance = .5
        queue_coverage = Counter()
        for i in range(repeats):
            # N workers for two queues, one queue shorter than the other
            queue_indices = self._test(num_workers=num_workers,
                                       queue_lengths=[100, imbalance * 100])
            # Every queue should be served by at least one worker (mandatory services)
            self.assertEquals(set(queue_indices), {0, 1})
            # Count how many times each queue was served
            queue_coverage.update(queue_indices)
        # Every worker in every iteration serves exactly one queue
        self.assertEquals(sum(queue_coverage.values()), repeats * num_workers)
        # Compute the probability of the longer queue being served by a surplus workers
        service_ratio = (queue_coverage[1] - repeats) / (queue_coverage[0] - repeats)
        # The probability should be equal to the imbalance, within an epsilon
        self.assertAlmostEqual(service_ratio, imbalance, delta=.1)

    def test_worker_shortage(self):
        num_queues = 10
        num_workers = 2
        repeats = 1000
        imbalance = .5
        queue_coverage = Counter()
        for i in range(repeats):
            queue_indices = self._test(num_workers=num_workers,
                                       queue_lengths=[100] + [round(imbalance * 100)] * (num_queues - 1))
            queue_coverage.update(queue_indices)
        # Every worker in every iteration serves exactly one queue
        self.assertEquals(sum(queue_coverage.values()), repeats * num_workers)
        first_queue_coverage = queue_coverage.pop(0)
        avg = sum(c for c in queue_coverage.values()) / (num_queues - 1)
        self.assertAlmostEqual(avg / first_queue_coverage, imbalance, delta=.1)
        sigma = sqrt(sum((c - avg) ** 2 for c in queue_coverage.values()) / (num_queues - 2))
        self.assertTrue(all(abs(c - avg) <= sigma) for c in queue_coverage.values())


# noinspection PyAbstractClass
class MockLambdaContext(LambdaContext):

    def __init__(self, running_time: float) -> None:
        super().__init__()
        self.end_time = time.time() + running_time

    def get_remaining_time_in_millis(self) -> int:
        return int((self.end_time - time.time()) * 1000)


class PostTestHandler(BaseHTTPRequestHandler):
    actual_receptions: List[str] = []
    hmac_secret_key = str(uuid.uuid4()).encode()

    @classmethod
    def reset(cls):
        cls.actual_receptions = []

    # noinspection PyAttributeOutsideInit
    def setup(self):
        super().setup()
        # Since we're messing with the connection timing, we make sure connections aren't reused on the client-side.
        self.close_connection = True

    response_body = os.urandom(1024 * 1024)

    def do_POST(self):
        length = int(self.headers['content-length'])
        attempt = int(self.headers[Notification.attempt_header_name])
        payload = json.loads(self.rfile.read(length).decode())
        verify = payload['verify']
        if verify:
            HTTPSignatureAuth.verify(requests.Request("POST", self.path, self.headers),
                                     key_resolver=lambda key_id, algorithm: self.hmac_secret_key)
            try:
                HTTPSignatureAuth.verify(requests.Request("POST", self.path, self.headers),
                                         key_resolver=lambda key_id, algorithm: self.hmac_secret_key[::-1])
            except AssertionError:
                pass
            else:
                raise AssertionError("Expected AssertionError")
        responses = payload['responses']
        delay, status = responses[attempt if attempt < len(responses) else -1]
        self.send_response(status)
        if delay:
            self.send_header("Content-length", len(self.response_body))
            self.end_headers()
            time.sleep(delay)
            # Write a lot of data to force the detection of a client disconnect. The connection is to the loopback
            # interface so this shouldn't matter much performance-wise. When the disconnect is detected,
            # the execptions raised range from the expected EPIPE to the exotic 'OSError: [Errno 41] Protocol wrong
            # type for socket'. We don't care which exception is raised as long as it prevents the request being
            # recorded as a success.
            try:
                self.wfile.write(self.response_body)
                self.wfile.flush()
            except OSError:
                logger.info("An expected exception occurred while sending response to client:", exc_info=True)
                return
        else:
            self.send_header("Content-length", 0)
            self.end_headers()

        if status == 200:
            notification_id = payload['notification_id']
            logger.info("Received notification_id %s", notification_id)
            self.actual_receptions.append(notification_id)

    def log_message(self, fmt, *args):
        logger.info("%s - - [%s] %s\n", self.address_string(), self.log_date_time_string(), fmt % args)


if __name__ == "__main__":
    unittest.main()

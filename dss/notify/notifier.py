import concurrent.futures
from itertools import chain
import logging
import math
import time
from typing import Iterable, List, Optional, Union

import boto3
import functools
import random

from dss import Config
from dss.notify.notification import Notification
from dss.util import require
from dss.util.time import RemainingTime

logger = logging.getLogger(__name__)

SQS_MAX_VISIBILITY_TIMEOUT = 43200


class Notifier:

    @classmethod
    def from_config(cls):
        """
        Create a Notifier instance with global configuration, typically environment variables.
        """
        kwargs = dict(deployment_stage=Config.deployment_stage(),
                      delays=Config.notification_delays(),
                      num_workers=Config.notification_workers(),
                      timeout=Config.notification_timeout())
        return cls(**{k: v for k, v in kwargs.items() if v is not None})

    def __init__(self,
                 deployment_stage: str,
                 delays: List[float],
                 num_workers: int = None,
                 sqs_polling_timeout: int = 20,
                 timeout: float = 10.0,
                 overhead: float = 30.0) -> None:
        """
        Create a new notifier.

        :param deployment_stage: the name of the deployment stage that hosts the cloud resources used by this
        notifier. Multiple notifier instances created with the same value for this parameter will share those
        resources.

        :param delays: a list of delays in seconds before notification attempts. There will be one SQS queue and
        typically one worker thread per entry in this list. The first value is typically zero, unless the first
        attempt should be delayed as well. Multiple notifier instances created with the same value for the
        `deployment_stage` parameter should have the same value for the delays parameter. If they don't,
        the resulting behavior is unspecififed.

        :param num_workers: the number of notification workers pulling notifications from the queues and delivering
        them. The default is to have one worker per queue. If the configured number of workers is less than the
        number of queues, the notifier will assign the workers to a weighted random choice among the queues,
        prioritizing longer queues. If the configured number of workers is greater than the number of queues,
        the notifier will first ensure that every queue is assigned one worker and then assign the remaining workers
        to a weighted random choice of queues, again prioritizing longer ones. Multiple notifier instances,
        each with their own set of workers, can be pulling from the same set of queues. If the number of workers is
        less than the number of queues, multiple notifier instances are needed to ensure that every queue is served.

        :param sqs_polling_timeout: the integral number of seconds that a SQS ReceiveMessages request waits on any
        queue before returning

        :param timeout: the notification timeout in seconds. A notification attempt will be considered a failure if
        the HTTP/HTTPS request to the corresponding notification endpoint takes longer than the specified amount of
        time.

        :param overhead: The expected amount of time needed for ancillary work around one notification attempt. This
        does not include the time for making the actual HTTP(S) notification request but includes dequeueing and
        potentially requeueing the notification. If this value is too low, duplicate notification attempts will be
        made. If it is too high, throughput might be slightly impaired on the happy path, and considerably impaired
        when workers (not the endpoints) fail and messages have to be requed by SQS VisibilityTimeout mechanism.
        """
        super().__init__()
        require(len(delays) > 0, "Must specify at least one delay value.")
        require(num_workers is None or num_workers > 0, "If specified, the number of workers must be greater than 0.")
        self._deployment_stage = deployment_stage
        self._delays = delays
        self._num_workers = len(delays) if num_workers is None else num_workers
        self._sqs_polling_timeout = sqs_polling_timeout
        self._timeout = timeout
        self._overhead = overhead

    def deploy(self) -> None:
        sqs = boto3.client('sqs')
        two_weeks = '1209600'
        for queue_index in self._queue_indices:
            sqs.create_queue(QueueName=self._queue_name(queue_index),
                             Attributes={'FifoQueue': 'true',
                                         'MessageRetentionPeriod': two_weeks})
        sqs.create_queue(QueueName=self._queue_name(None),
                         Attributes={'FifoQueue': 'true',
                                     'MessageRetentionPeriod': two_weeks})

    def destroy(self, purge_only=False) -> None:
        if self._deployment_stage.startswith('test-'):
            for queue_index in self._all_queue_indices:
                queue = self._queue(queue_index)
                if purge_only:
                    queue.delete_messages()
                else:
                    queue.delete()
        else:
            raise RuntimeError('Deletion of queues outside the test deployment stage is prohibited.')

    def enqueue(self, notification: Notification, queue_index: int = 0) -> None:
        require(notification.attempts is not None,
                "Cannot enqueue a notification whose `attempts` attribute is None", notification)
        if queue_index is None:
            # Enqueueing a notification into the failure queue does not consume an attempt.
            logger.info(f"Adding notification to '{self._queue_name(None)}' queue as requested: {notification}")
        elif notification.attempts > 0:
            queues_left = self.num_queues - queue_index
            if notification.attempts < queues_left:
                logger.info(f"Notification would expire before proceeding through the remaining queues (%i): %s",
                            queues_left, notification)
            # Enqueueing a notification into one of the regular queues consumes one attempt.
            notification = notification.spend_attempt()
            logger.info(f"Adding notification to queue {queue_index}: {notification}")
        else:
            logger.warning(f"Notification has no attempts left, giving up. Adding it to '{self._queue_name(None)}' "
                           f"instead of ({self._queue_name(queue_index)}) as originally requested: {notification}")
            queue_index = None
        queue = self._queue(queue_index)
        queue.send_message(**notification.to_sqs_message())

    def run(self, remaining_time: RemainingTime) -> None:
        queue_lengths = self._sample_queue_lengths()
        queue_indices = self._work_queue_indices(queue_lengths)
        with concurrent.futures.ThreadPoolExecutor(self._num_workers) as pool:
            future_to_worker = {}
            for worker_index, queue_index in enumerate(queue_indices):
                remaining_worker_time = RemainingWorkerTime(remaining_time, worker_index)
                worker = functools.partial(self._worker, worker_index, queue_index, remaining_worker_time)
                future = pool.submit(worker)
                future_to_worker[future] = worker_index
            exceptions = {}
            for future in concurrent.futures.as_completed(future_to_worker.keys()):
                worker_index = future_to_worker[future]
                assert future.done()
                e = future.exception()
                if e:
                    logger.error("Exception in worker %i: ", worker_index, exc_info=e)
                    exceptions[worker_index] = e
            if exceptions:
                raise RuntimeError(exceptions)

    _failure_queue_name_suffix = "fail"

    _queue_name_suffix = ".fifo"

    def _work_queue_indices(self, queue_lengths):
        # random.choices() does not handle sum(weights) == 0 so we account for that by adding one to every weight.
        # This will skew the weights a little bit, creating an bias in favor of shorter queues. If all queues are
        # short the bias will be significant, albeit short-lived because the queues will be consumed quickly. If the
        # queues are long, the bias is insignificant.
        weights = [l + 1 for l in queue_lengths]
        assert all(0 < w for w in weights)
        if self._num_workers < self.num_queues:
            # If there aren't enough workers to serve all queues, randomly select queues, prefering longer ones.
            queue_indices = random.choices(k=self._num_workers,
                                           weights=weights,
                                           population=self._queue_indices)
        elif self._num_workers == self.num_queues:
            queue_indices = self._queue_indices
        else:
            # With fewer queues than workers, ensure that each queue is worked on and distribute the remaining
            # workers to a weighted random selection of queues, again preferring longer queues over shorter ones.
            queue_indices = chain(self._queue_indices, random.choices(k=self._num_workers - self.num_queues,
                                                                      weights=weights,
                                                                      population=self._queue_indices))
        return queue_indices

    def _sample_queue_lengths(self) -> List[int]:
        sqs = boto3.resource('sqs')
        queues = sqs.queues.filter(QueueNamePrefix=self._queue_name_prefix)
        queue_lengths: List[Optional[int]] = [None] * self.num_queues
        for queue in queues:
            queue.load()
            queue_name = queue.attributes['QueueArn'].rsplit(':', 1)[1]
            suffix = self._queue_index(queue_name)
            if suffix is not None:
                queue_index = int(suffix)
                if queue_index < self.num_queues:
                    queue_lengths[queue_index] = int(queue.attributes['ApproximateNumberOfMessages'])
                else:
                    logger.warning("Index of queue %s is out of the configured bounds. It will be ignored.", queue_name)
        for queue_index, queue_length in enumerate(queue_lengths):
            if queue_length is None:
                raise RuntimeError(f"Missing queue with index {queue_index}")
        return queue_lengths

    def _worker(self, worker_index: int, queue_index: int, remaining_time: RemainingTime) -> None:

        queue = self._queue(queue_index)
        while self._timeout + self._sqs_polling_timeout < remaining_time.get():
            visibility_timeout = self._timeout + self._overhead
            messages = queue.receive_messages(MaxNumberOfMessages=1,
                                              WaitTimeSeconds=self._sqs_polling_timeout,
                                              AttributeNames=['All'],
                                              MessageAttributeNames=['*'],
                                              VisibilityTimeout=int(math.ceil(visibility_timeout)))
            if messages:
                assert len(messages) == 1
                message = messages[0]
                notification = Notification.from_sqs_message(message)
                logger.info(f'Worker {worker_index} received message from queue {queue_index} for {notification}')
                seconds_to_maturity = notification.queued_at + self._delays[queue_index] - time.time()
                if seconds_to_maturity > 0:
                    # Hide the message and sleep until it matures. These two measures prevent immature messages from
                    # continuously bouncing between the queue and the workers consuming it, thereby preventing
                    # unnecessary churn on the queue. Consider that other messages further up in the queue
                    # invariantly mature after the current message, ensuring that this wait does not limit throughput
                    # or increase latency.
                    #
                    # TODO: determine how this interacts with FIFO queues and message groups as those yield
                    # messages in an ordering that, while being strict with respect to a group, is only partial
                    # with respect to the entire queue. The above invariant may not hold globally for that reason.
                    #
                    # SQS ignores a request to change the VTO of a message if the total VTO would exceed the max.
                    # allowed value of 12 hours. To be safe, we subtract the initial VTO from the max VTO.
                    max_visibility_timeout = SQS_MAX_VISIBILITY_TIMEOUT - visibility_timeout
                    visibility_timeout = min(seconds_to_maturity, max_visibility_timeout)
                    logger.info(f'Worker {worker_index} hiding message from queue {queue_index} '
                                f'for another {visibility_timeout:.3f}s. '
                                f'It will be {seconds_to_maturity:.3f}s to maturity of {notification}.')
                    message.change_visibility(VisibilityTimeout=int(visibility_timeout))
                    time.sleep(min(seconds_to_maturity, remaining_time.get()))
                elif remaining_time.get() < visibility_timeout:
                    logger.info(f'Worker {worker_index} returning message to queue {queue_index}. '
                                f'There is not enough time left to deliver {notification}.')
                    message.change_visibility(VisibilityTimeout=0)
                else:
                    if not notification.deliver(timeout=self._timeout, attempt=queue_index):
                        self.enqueue(notification, self._next_queue_index(queue_index))
                    message.delete()
        else:
            logger.debug(f"Exiting worker {worker_index} due to insufficient time left.")

    @property
    def _queue_name_prefix(self):
        return f"dss-notify-{self._deployment_stage}-"

    def _queue_name(self, queue_index: Optional[int]):
        _queue_index = self._failure_queue_name_suffix if queue_index is None else str(queue_index)
        return self._queue_name_prefix + _queue_index + self._queue_name_suffix

    @property
    def _queue_indices(self) -> Iterable[int]:
        return range(self.num_queues)

    @property
    def _all_queue_indices(self) -> Iterable[Union[int, None]]:
        return chain(self._queue_indices, (None,))

    @property
    def num_queues(self) -> int:
        return len(self._delays)

    def _queue(self, queue_index: Optional[int]):
        return boto3.resource("sqs").get_queue_by_name(QueueName=self._queue_name(queue_index))

    def _next_queue_index(self, queue_index: int) -> Optional[int]:
        assert isinstance(queue_index, int) and 0 <= queue_index
        return queue_index + 1 if queue_index < self.num_queues - 1 else None

    def _queue_index(self, queue_name) -> Optional[int]:
        prefix, suffix = self._queue_name_prefix, self._queue_name_suffix
        assert queue_name.startswith(prefix) and queue_name.endswith(suffix)
        queue_index = queue_name[len(prefix):-len(suffix)]
        return None if queue_index == self._failure_queue_name_suffix else int(queue_index)


class RemainingWorkerTime(RemainingTime):

    def __init__(self, actual: RemainingTime, worker_index: int) -> None:
        super().__init__()
        self._worker_index = worker_index
        self._actual = actual

    def get(self) -> float:
        value = self._actual.get()
        logger.debug(f"Remaining runtime for worker {self._worker_index}: {value:.3f}s")
        return value

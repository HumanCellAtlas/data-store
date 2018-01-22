import string
from time import time
from cloud_blobstore import BlobPagingError
from typing import Sequence, MutableMapping, Mapping
from collections import Counter

from dss.events.handlers.index import Indexer
from .timeout import Timeout
from ...config import Config, Replica
from . import Visitation, WalkerStatus


class Reindex(Visitation):
    """
    Reindex batch job.
    """

    state_spec = {
        'replica': None,
        'bucket': None,
        'dryrun': None,
        'notify': None,
        'work_result': {
            'processed': 0,
            'indexed': 0,
            'failed': 0
        }
    }

    walker_state_spec = {
        'marker': None,
        'token': None
    }

    def job_initialize(self):
        prefix_chars = list(set(string.hexdigits.lower()))
        if self._number_of_workers <= 16:
            self.work_ids = prefix_chars
        else:
            self.work_ids = [a + b for a in prefix_chars for b in prefix_chars]

    def process_item(self, key):
        self.work_result['processed'] += 1
        try:
            self.indexer.index_object(key, self.logger)
        except Exception:
            self.work_result['failed'] += 1
            self.logger.warning(f'Reindex operation failed for {key}', exc_info=True)
        else:
            self.work_result['indexed'] += 1

    def walker_finalize(self):
        self.logger.info(f'Work result: {self.work_result}')
        self.marker = None
        self.token = None

    def _walk(self, seconds_allowed=250) -> None:
        start_time = time()

        indexer_class = Indexer.for_replica[Replica[self.replica]]

        self.indexer = indexer_class(dryrun=self.dryrun, notify=self.notify)

        handle, _, default_bucket = Config.get_cloud_specific_handles_DEPRECATED(Replica[self.replica])

        if self.bucket != default_bucket:
            self.logger.warning(f'Indexing bucket {self.bucket} instead of default {default_bucket}.')

        blobs = handle.list_v2(
            self.bucket,
            prefix=f'bundles/{self.work_id}',
            start_after_key=self.marker,  # type: ignore  # Cannot determine type of 'marker'
            token=self.token  # type: ignore  # Cannot determine type of 'token'
        )

        for key in blobs:
            seconds_remaining = int(seconds_allowed - (time() - start_time))

            if 1 > seconds_remaining:
                self.logger.info(f'{self.work_id} timed out before reindex')
                return

            with Timeout(seconds_remaining) as timeout:
                """
                Timing out while recording paging info could cause an inconsistent paging state, leading
                to repeats of large amounts of work. This can be avoided by checking for timeouts only
                during actual re-indexing.
                """
                self.process_item(key)

            if timeout.did_timeout:
                self.logger.warning(f'{self.work_id} timed out during reindex')
                return

            self.marker = blobs.start_after_key
            self.token = blobs.token
        else:
            self._status = WalkerStatus.finished.name

    def walker_walk(self) -> None:
        try:
            self._walk()
        except BlobPagingError:
            self.marker = None
            self._walk()

    def _aggregate(self, work_results: Sequence[Mapping[str, int]]) -> MutableMapping[str, int]:
        aggregate: MutableMapping[str, int] = Counter()
        for work_result in work_results:
            aggregate.update(work_result)
        return dict(aggregate)

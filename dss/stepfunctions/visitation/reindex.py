from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import logging
import string
from typing import Mapping, MutableMapping, Sequence

from cloud_blobstore import BlobPagingError

from dss.config import Config, Replica
from dss.index import DEFAULT_BACKENDS
from dss.index.backend import CompositeIndexBackend
from dss.index.indexer import Indexer, IndexerTimeout
from . import Visitation, WalkerStatus

logger = logging.getLogger(__name__)


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

    def walker_finalize(self):
        logger.info(f'Work result: {self.work_result}')
        self.marker = None
        self.token = None

    def _walk(self) -> None:
        executor = ThreadPoolExecutor(len(DEFAULT_BACKENDS))
        # We can't use executor as context manager because we don't want shutting it down to block
        try:
            backend = CompositeIndexBackend(executor, DEFAULT_BACKENDS, dryrun=self.dryrun, notify=self.notify)
            indexer_cls = Indexer.for_replica(Replica[self.replica])
            indexer = indexer_cls(backend, self._context)

            handle = Config.get_blobstore_handle(Replica[self.replica])
            default_bucket = Replica[self.replica].bucket

            if self.bucket != default_bucket:
                logger.warning(f'Indexing bucket {self.bucket} instead of default {default_bucket}.')

            blobs = handle.list_v2(
                self.bucket,
                prefix=f'bundles/{self.work_id}',
                start_after_key=self.marker,  # type: ignore  # Cannot determine type of 'marker'
                token=self.token  # type: ignore  # Cannot determine type of 'token'
            )

            for key in blobs:
                # Timing out while recording paging info could cause an inconsistent paging state, leading to repeats
                # of large amounts of work. This can be avoided by checking for timeouts only during actual
                # re-indexing. The indexer performs this check for every item.
                self.work_result['processed'] += 1
                try:
                    indexer.index_object(key)
                except IndexerTimeout as e:
                    self.work_result['failed'] += 1
                    logger.warning(f'{self.work_id} timed out during reindex: {e}')
                    break
                except Exception:
                    self.work_result['failed'] += 1
                    logger.warning(f'Reindex operation failed for {key}', exc_info=True)
                else:
                    self.work_result['indexed'] += 1
                    self.marker = blobs.start_after_key
                    self.token = blobs.token
            else:
                self._status = WalkerStatus.finished.name
        finally:
            executor.shutdown(False)

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

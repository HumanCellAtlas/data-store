import logging
import string
from typing import Mapping, MutableMapping, Sequence, List, Optional, Tuple

from cloud_blobstore import PagedIter

from dss.config import Config, Replica
from dss.util.iterators import zipalign
from . import Visitation, WalkerStatus

logger = logging.getLogger(__name__)


# noinspection PyAttributeOutsideInit
class StorageVisitation(Visitation):

    # A central pattern in this class is to use covariant arrays that have one element per replica. For example,
    # `self.buckets[i]` is the custom bucket to be used for the replica whose name is `self.replicas[i]`. Similarly,
    # `self.work_result['missing'][i]` is the number of missing keys in the bucket for the replica whose name is
    # `self.replicas[i]`. While this is not especially readable, it is efficient and fits best with the convention
    # established by the super class which encourages state to be a dict with static keys. Instead of arrays I
    # experimented with dicts keyed on replica name but that turned out to be unwieldy. The covariant array approach
    # allows for the use of zip() to merge two such arrays and iterate over the result. The most readable and
    # efficient pattern would be to use a single dict that maps replica names to objects or dicts representing all
    # replica-specific items, but again, that approach didn't seem to fit well with the superclass.

    shutdown_time = 10

    state_spec = dict(replicas=None,
                      buckets=None,
                      work_result=dict(present=None,
                                       missing=None))

    prefix_chars = list(set(string.hexdigits.lower()))

    def job_initialize(self):
        if self._number_of_workers <= 16:
            self.work_ids = self.prefix_chars
        else:
            self.work_ids = [a + b for b in self.prefix_chars for a in self.prefix_chars]
        n = len(self.replicas)
        self.work_result = {k: [0] * n for k in self.work_result.keys()}

    walker_state_spec = dict(tokens=None,
                             row=None)

    def walker_initialize(self) -> None:
        n = len(self.replicas)
        self.row: Tuple[Optional[str], ...] = (None,) * n
        self.tokens = [None] * n

    def walker_walk(self) -> None:
        columns = []
        for replica, bucket, key, token in zip(self.replicas, self.buckets, self.row, self.tokens):
            replica = Replica[replica]
            if bucket is None:
                bucket = replica.bucket
            elif bucket != replica.bucket:
                logger.warning(f'Checking bucket {bucket} instead of default {replica.bucket} for replica {replica}.')
            handle = Config.get_blobstore_handle(replica)
            column: PagedIter = handle.list_v2(bucket,
                                               prefix='bundles/' + self.work_id,
                                               token=token,
                                               start_after_key=key)
            columns.append(column)

        diff = zipalign(columns=map(iter, columns), row=self.row)
        while self.shutdown_time < self.remaining_runtime():
            try:
                row = next(diff)
            except StopIteration:
                logger.info("Finished checking replicas.")
                self._status = WalkerStatus.finished.name
                break
            else:
                for i, key in enumerate(row.norm()):
                    replica = self.replicas[i]
                    if key is None:
                        logger.warning(f"Replica {replica} is missing {row.min}")
                        self.work_result['missing'][i] += 1
                    else:
                        logger.debug(f"Replica {replica} contains {key}")
                        self.work_result['present'][i] += 1
                self.row = row.values
        else:
            self.tokens = [column.token for column in columns]
            logger.debug('Not enough time left in lambda execution, exiting.')

    def walker_finalize(self):
        self.tokens = None
        self.row = None
        logger.info(f'Work result: {self.work_result}')

    def _aggregate(self, work_results: Sequence[Mapping[str, List[int]]]) -> MutableMapping[str, List[int]]:
        aggregate: MutableMapping[str, List[int]] = {}
        for work_result in work_results:
            for name, deltas in work_result.items():
                try:
                    counters = aggregate[name]
                except KeyError:
                    aggregate[name] = list(deltas)
                else:
                    for i, delta in enumerate(deltas):
                        counters[i] += delta
        return aggregate

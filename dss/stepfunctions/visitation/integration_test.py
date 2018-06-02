import logging
import string
from time import time
from typing import Sequence, Any

from cloud_blobstore import BlobPagingError

from dss.config import Config, Replica
from . import Visitation, WalkerStatus


logger = logging.getLogger(__name__)


class IntegrationTest(Visitation):  # no coverage (this code *is* run by tests, just only on Lambda)
    """
    Test of the visitation batch processing architecture.
    """

    prefix = 'bundles/'

    state_spec = {
        'replica': None,
        'bucket': None,
        'work_result': int
    }

    walker_state_spec = {
        'marker': None,
        'token': None
    }

    def job_initialize(self):
        prefix_chars = set(string.hexdigits.lower())
        self.work_ids = [self.prefix + a for a in prefix_chars]

    def process_item(self, key, walker_data):
        walker_data['count'] += 1

    def job_finalize(self):
        handle = Config.get_blobstore_handle(Replica[self.replica])

        work_ids = [work_id for work_id_group in self.work_ids for work_id in work_id_group]

        for work_id in work_ids:
            count = self.get_persistent_data(work_id)['count']
            listed_keys = handle.list(self.bucket, prefix=work_id)
            k_listed = sum(1 for _ in listed_keys)
            assert count == k_listed, f'Integration test failed: {self.work_result} != {k_listed}'
            logger.info(f"Integration test passed for {self.replica} with {k_listed} key(s) listed")

    def _walk(self, walker_data) -> None:
        """
        Subclasses should not typically implement this method, which includes logic specific to calling
        self.process_item(*args) on each blob visited.
        """

        start_time = time()

        handle = Config.get_blobstore_handle(Replica[self.replica])

        blobs = handle.list_v2(
            self.bucket,
            prefix=self.work_id,
            start_after_key=self.marker,  # type: ignore  # Cannot determine type of 'marker'
            token=self.token  # type: ignore  # Cannot determine type of 'token'
        )

        for key in blobs:
            if 250 < time() - start_time:
                break
            self.process_item(key, walker_data)
            self.marker = blobs.start_after_key
            self.token = blobs.token
        else:
            self._status = WalkerStatus.finished.name

    def walker_walk(self) -> None:
        walker_data = self.get_persistent_data(self.work_id)
        if not walker_data.get('count', None):
            walker_data['count'] = 0

        try:
            self._walk(walker_data)
        except BlobPagingError:
            self.marker = None
            self._walk(walker_data)

        self.put_persistent_data(self.work_id, walker_data)

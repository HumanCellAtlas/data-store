
import string
from time import time
from . import Visitation
from ...config import Config, Replica
from cloud_blobstore import BlobPagingError


class IntegrationTest(Visitation):
    """
    Test of the vistation batch processing architecture.
    """

    common_state_spec = {
        'replica': None,
        'bucket': None,
    }

    sentinel_state_spec = {}

    walker_state_spec = {
        'marker': None,
        'token': None,
        'number_of_keys_processed': int,
    }

    def sentinel_initialize(self):
        alphanumeric = string.ascii_lowercase[:6] + '0987654321'
        self.work_ids = [f'files/{a}' for a in alphanumeric]

    def process_item(self, key):
        self.number_of_keys_processed = self.number_of_keys_processed + 1

    def walker_finalize(self):
        handle, _, _ = Config.get_cloud_specific_handles(Replica[self.replica])
        listed_keys = handle.list(self.bucket, self.work_id)
        k_listed = len(list(listed_keys))

        if self.number_of_keys_processed != k_listed:
            raise Exception(f'Integration test failed {self.number_of_keys_processed} {k_listed}')

    def _walk(self) -> None:
        """
        Subclasses should not typically impliment this method, which includes logic specific to calling
        self.process_item(*args) on each blob visited.
        """

        start_time = time()

        handle = Config.get_cloud_specific_handles(Replica[self.replica])[0]

        blobs = handle.list_v2(
            self.bucket,
            prefix=self.work_id,
            start_after_key=self.marker,  # type: ignore  # Cannot determine type of 'marker'
            token=self.token  # type: ignore  # Cannot determine type of 'token'
        )

        self.is_finished = False

        for key in blobs:
            if 250 < time() - start_time:
                break
            self.process_item(key)
            self.marker = blobs.start_after_key
            self.token = blobs.token
        else:
            self.is_finished = True

    def walker_walk(self) -> None:
        try:
            self._walk()
        except BlobPagingError:
            self.marker = None
            self._walk()

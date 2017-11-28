
import time
import string
from .utils import validate_bucket
from . import Visitation, StatusCode
from .blobstore import BlobListerizer


class IntegrationTest(Visitation):

    walker_state_spec = {
        ** Visitation.walker_state_spec,
        ** dict(
            processed_keys = list
        )
    }

    def initialize(self):

        validate_bucket(
            self.bucket
        )

        alphanumeric = string.ascii_lowercase[:6] + '0987654321'

        self.waiting = [f'{a}{b}' for a in alphanumeric for b in alphanumeric]

        self.code = StatusCode.RUNNING.name


    def finalize(self):
        pass


    def finalize_failed(self):
        pass


    def initialize_walker(self):
        
        validate_bucket(
            self.bucket
        )

        self.code = StatusCode.RUNNING.name


    def process_item(self, key):
        self.processed_keys.append(
            key
        )


    def walk(self):
        
        self.k_starts += 1
        elapsed_time = 0
        start_time = time.time()

        blobs = BlobListerizer(
            self.replica,
            self.bucket,
            self.dirname + '/' + self.prefix,
            self.marker
        )

        for key in blobs:
            try:
                self.process_item(
                    key
                )

            except DSSVisitationExceptionSkipItem as e:
                self.logger.warning(e)

            self.k_processed += 1
            self.marker = blobs.marker
            self.token = blobs.token

            if time.time() - start_time >= self._walker_timeout:
                self.code = StatusCode.RUNNING.name
                break

        else:
            self.code = StatusCode.SUCCEEDED.name


    def finalize_walker(self):
        pass


    def finalize_failed_walker(self):
        pass

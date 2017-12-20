
import json
import boto3
import string
from ...config import Config, Replica
from .utils import walker_execution_name
from . import Visitation, StatusCode
from .. import step_functions_invoke, step_functions_describe_execution


class IntegrationTest(Visitation):
    """
    Test of the vistation batch processing architecture.
    """

    sentinel_state_spec = {
        'number_of_keys_processed': int,
        'wait_time': 2
    }

    walker_state_spec = {
        'processed_keys': list
    }

    def sentinel_initialize(self):
        alphanumeric = string.ascii_lowercase[:6] + '0987654321'
        self.prefixes = [f'files/{a}' for a in alphanumeric]
        self.code = StatusCode.RUNNING.name

    def process_item(self, key):
        self.processed_keys.append(key)

    def sentinel_finalize(self):
        processed_keys = list()
        for pfx in self.prefixes:
            walker_name = walker_execution_name(self.name, pfx)
            resp = step_functions_describe_execution('dss-visitation-{stage}', walker_name)
            output = json.loads(resp.get('output', '{}'))
            processed_keys.extend(output.get('processed_keys', []))

        handle, _, _ = Config.get_cloud_specific_handles(Replica[self.replica])
        listed_keys = handle.list(self.bucket, 'files')

        if set(processed_keys) != set(listed_keys):
            raise Exception('Integration test failed :(')

        self.number_of_keys_processed = len(processed_keys)

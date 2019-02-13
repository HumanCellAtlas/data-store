import json
import os
from dss.storage.hcablobstore import FileMetadata

"""
These functions assist with the caching process, the lifecycle policies are set to delete files within
the checkout buckets;
For AWS object tagging is used to mark files for deletion: TagSet=[{uncached:True}]
For GCP object classes are used to indicate what is to be cached:
See MetaData Caching RFC for more information (Google Docs)
"""


def _cache_net():
    with open("checkout_cache_criteria.json", "r") as file:
        temp = json.load(file)
    return temp


def _simulated_net():
    return json.loads('[{"type": "application/json","max_size": 12314}]')


def dss_managed_checkout_bucket(bucket):
    return 'dss-checkout' in bucket


def get_cached_status(file_metadata: dict):
    """Returns True if a file should be cached (marked as long-lived) for the dss checkout bucket."""
    # Each file type may have a size limit that determines uncached status.
    for file_type in _simulated_net():
        if file_type['type'] == file_metadata[FileMetadata.CONTENT_TYPE]:
            if file_type['max_size'] >= file_metadata[FileMetadata.SIZE]:
                return True
    return False

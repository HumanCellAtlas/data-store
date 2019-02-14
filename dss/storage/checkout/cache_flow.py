import json
import os
from dss.storage.hcablobstore import FileMetadata

"""
These functions assist with the caching process, the lifecycle policies are set to delete files within
the checkout buckets;
For AWS object tagging is used to mark files for deletion: TagSet=[{uncached:True}]
For GCP object classes are used to indicate what is to be cached: STANDARD_STORAGE (MULTI-REGIONAL) are cached
See MetaData Caching RFC for more information (Google Docs)
"""


def check_dss_bucket(dst_bucket: str):
    return "dss-checkout" in dst_bucket.lower()


def get_local_criteria():
    with open("{}/checkout_cache_criteria.json".format(os.environ['DSS_HOME']), "r") as file:
        criteria = json.load(file)
    return criteria


def get_cached_status(file_metadata: dict):
    """Returns True if a file should be cached (marked as long-lived) for the dss checkout bucket."""
    # Each file type may have a size limit that determines uncached status.
    if os.getenv("CHECKOUT_CACHE_CRITERIA") is None:
        cache_criteria = get_local_criteria()
    else:
        cache_criteria = json.loads(os.getenv('CHECKOUT_CACHE_CRITERIA'))
    for file_type in cache_criteria:
        if file_type['type'] == file_metadata[FileMetadata.CONTENT_TYPE]:
            if file_type['max_size'] >= file_metadata[FileMetadata.SIZE]:
                return True
    return False

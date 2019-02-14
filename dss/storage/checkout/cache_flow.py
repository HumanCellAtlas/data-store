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
    return dst_bucket in (os.environ['DSS_GS_CHECKOUT_BUCKET_TEST'], os.environ['DSS_S3_CHECKOUT_BUCKET_TEST'],
                          os.environ['DSS_S3_CHECKOUT_BUCKET'], os.environ['DSS_GS_CHECKOUT_BUCKET'])


def get_cache_criteria():
    """Fetch criteria to determine if files should be cached."""
    if os.getenv("CHECKOUT_CACHE_CRITERIA"):
        return json.loads(os.getenv('CHECKOUT_CACHE_CRITERIA'))

    local_cache_criteria_path = f'{os.environ["DSS_HOME"]}/checkout_cache_criteria.json'
    print(f'CHECKOUT_CACHE_CRITERIA is not set.  Default cache criteria will be pulled from: {local_cache_criteria_path}')
    with open(local_cache_criteria_path, 'r') as file:
        criteria = json.load(file)
    return criteria


def get_cached_status(file_metadata: dict):
    """Returns True if a file should be cached (marked as long-lived) for the dss checkout bucket."""
    # Each file type may have a size limit that determines uncached status.
    cache_criteria = get_cache_criteria()
    for file_type in cache_criteria:
        if file_type['type'] == file_metadata[FileMetadata.CONTENT_TYPE]:
            if file_type['max_size'] >= file_metadata[FileMetadata.SIZE]:
                return True
    return False

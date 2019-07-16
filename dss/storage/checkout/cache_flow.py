import json
import os
"""
These functions assist with the caching process and provide greater availability of heavily accessed files to the user.

The criteria used to determine if a file should be cached or not is set with: CHECKOUT_CACHE_CRITERIA
For example: CHECKOUT_CACHE_CRITERIA='[{"type":"application/json","max_size":12314}]'

Uncached files are controlled by a lifecycle policy that deletes them regularly.  Cached files are ignored by
this lifecycle policy and are (currently) never deleted.

For AWS object tagging is used to mark uncached files: TagSet=[{uncached:True}]
For GCP object storage classes are used to indicate what is to be cached: STANDARD (MULTI_REGIONAL) are cached

Metadata Caching RFC: https://docs.google.com/document/d/1PQBO5qYUVJFAXFNaMdgxq8j0y-OI_EF2b15I6fvEYjo
"""


def is_dss_bucket(dst_bucket: str):
    """Function checks if the passed bucket is managed by the DSS"""
    return dst_bucket in (os.environ['DSS_S3_CHECKOUT_BUCKET'], os.environ['DSS_GS_CHECKOUT_BUCKET'])


def should_cache_file(content_type: str, size: int) -> bool:
    """Returns True if a file should be cached (marked as long-lived) for the dss checkout bucket."""
    # Each file type may have a size limit that determines uncached status.
    cache_criteria = json.loads(os.getenv("CHECKOUT_CACHE_CRITERIA"))
    for file_criteria in cache_criteria:
        if content_type.startswith(file_criteria['type']) and file_criteria['max_size'] >= size:
            return True
    return False

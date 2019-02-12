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


def dss_managed_checkout_bucket(bucket):
    return bucket in (os.environ['DSS_S3_CHECKOUT_BUCKET'], os.environ['DSS_GS_CHECKOUT_BUCKET'])


def get_cached_status(file_metadata: dict):
    """Returns True if a file should be cached (marked as long-lived) for the dss checkout bucket."""

    # Files can be checked out to a user bucket or the standard dss checkout bucket.
    # We return True if this is a user bucket because uncached files are unmodified
    # by either object tagging (AWS) or storage type changes (Google).
    # We are thus shielding the user from these intrusions upon their objects.
    if not dss_managed_checkout_bucket(file_metadata['Destination Bucket']):
        return True

    # That are over the size limit have an uncached status
    for file_type in _cache_net():
        if file_type['type'] == file_metadata[FileMetadata.CONTENT_TYPE]:
            if file_type['max_size'] >= file_metadata[FileMetadata.SIZE]:
                return True
    return False

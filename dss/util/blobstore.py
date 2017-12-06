from cloud_blobstore import BlobStore
from cloud_blobstore import BlobNotFoundError


def test_object_exists(handle: BlobStore, bucket: str, criteria: str, test_type: str = 'exact') -> bool:
    """
    Returns true if objects exist for the given prefix or name in the given bucket
    """
    if test_type == 'prefix':
        try:
            handle.list(bucket, prefix=criteria).__iter__().__next__()
        except StopIteration:
            return False
        else:
            return True
    elif test_type == 'exact':
        try:
            handle.get_user_metadata(bucket, criteria)
            return True
        except BlobNotFoundError:
            return False
    else:
        raise Exception(f"Not a valid storage object test type: " + test_type)

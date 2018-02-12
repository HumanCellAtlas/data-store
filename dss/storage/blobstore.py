from cloud_blobstore import BlobStore
from cloud_blobstore import BlobNotFoundError
from enum import Enum, auto


class ObjectTest(Enum):
    EXACT = auto()
    PREFIX = auto()


def test_object_exists(blobstore: BlobStore, bucket: str, match: str, test_type: ObjectTest = ObjectTest.EXACT) -> bool:
    """
    Test if an object exists in the BlobStore
    :param blobstore: the blobstore to check
    :param bucket: the bucket to check in the blobstore
    :param match: the string to match against; this is _not_ a regex pattern, strings must match exactly
    :param test_type: the type of test to conduct, prefix matches test if the object name starts with the match string,
        exact matches must match the full string
    :return: test bool
    """
    if test_type == ObjectTest.PREFIX:
        try:
            blobstore.list(bucket, prefix=match).__iter__().__next__()
        except StopIteration:
            return False
        else:
            return True
    elif test_type == ObjectTest.EXACT:
        try:
            blobstore.get_user_metadata(bucket, match)
            return True
        except BlobNotFoundError:
            return False
    else:
        raise ValueError(f"Not a valid storage object test type: " + test_type.name)

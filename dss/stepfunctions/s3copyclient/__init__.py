from .implementation import Key, sfn


def copy_sfn_event(source_bucket: str, source_key: str, destination_bucket: str, destination_key: str) -> dict:
    """Returns the initial event object to start the s3-s3 copy stepfunction."""
    return {
        Key.SOURCE_BUCKET: source_bucket,
        Key.SOURCE_KEY: source_key,
        Key.DESTINATION_BUCKET: destination_bucket,
        Key.DESTINATION_KEY: destination_key,
    }

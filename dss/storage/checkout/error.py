class TokenError(Exception):
    """Raised when we can't parse the token or it is missing fields."""
    pass


class CheckoutError(Exception):
    """Raised when the checkout fails."""
    pass


class PreExecCheckoutError(CheckoutError):
    """Raised when one of the quick checks before we start the checkout fails."""
    pass


class BundleNotFoundError(PreExecCheckoutError):
    """Raised when we attempt to check out a non-existent bundle."""
    pass


class DestinationBucketNotFoundError(PreExecCheckoutError):
    """Raised when we attempt to check out to a non-existent bucket."""
    pass


class DestinationBucketNotWritableError(PreExecCheckoutError):
    """Raised when we attempt to check out to a bucket that we can't write to."""
    pass

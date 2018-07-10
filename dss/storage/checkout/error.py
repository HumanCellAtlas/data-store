class BundleNotFoundError(Exception):
    """Raised when we attempt to check out a non-existent bundle."""
    pass


class TokenError(Exception):
    """Raised when we can't parse the token or it is missing fields."""
    pass


class CheckoutError(Exception):
    """Raised when the checkout fails."""
    pass

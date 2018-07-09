STATE_MACHINE_NAME_TEMPLATE = "dss-checkout-sfn-{stage}"


class EventConstants:
    """Externally visible constants used in the SFN messages."""

    EXECUTION_ID = "execution_id"
    BUNDLE_UUID = "bundle"
    BUNDLE_VERSION = "version"
    DSS_BUCKET = "dss_bucket"
    DST_BUCKET = "bucket"
    STATUS_BUCKET = "checkout_status_bucket"
    REPLICA = "replica"
    STATUS = "status"
    EMAIL = "email"

    STATUS_COMPLETE_COUNT = "complete_count"
    STATUS_TOTAL_COUNT = "total_count"
    STATUS_CHECK_COUNT = "check_count"
    STATUS_OVERALL_STATUS = "checkout_status"

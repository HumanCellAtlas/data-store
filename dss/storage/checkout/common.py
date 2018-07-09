import logging
import uuid

from dss import stepfunctions
from dss.config import Replica
from dss.stepfunctions import s3copyclient, gscopyclient


log = logging.getLogger(__package__)


class CheckoutTokenKeys:
    """
    When we are executing a request that involves a checkout, the client will periodically check back in to see if the
    checkout is complete.  To avoid duplicating checkout requests, the client will check back using a token.  These are
    keys that make up the token.
    """
    EXECUTION_ID = "execution_id"
    """Execution ID of the step function managing the checkout."""

    START_TIME = "start_time"
    """Start time of the request."""

    ATTEMPTS = "attempts"
    """Number of times the client has attempted to check on the state of a checkout."""


def get_execution_id() -> str:
    return str(uuid.uuid4())


def parallel_copy(
        replica: Replica,
        source_bucket: str,
        source_key: str,
        destination_bucket: str,
        destination_key: str) -> str:
    log.debug(f"Copy file from bucket {source_bucket} with key {source_key} to "
              f"bucket {destination_bucket} destination file: {destination_key}")

    if replica == Replica.aws:
        state = s3copyclient.copy_sfn_event(
            source_bucket, source_key,
            destination_bucket, destination_key,
        )
        state_machine_name_template = "dss-s3-copy-sfn-{stage}"
    elif replica == Replica.gcp:
        state = gscopyclient.copy_sfn_event(
            source_bucket, source_key,
            destination_bucket, destination_key
        )
        state_machine_name_template = "dss-gs-copy-sfn-{stage}"
    else:
        raise ValueError("Unsupported replica")

    execution_id = get_execution_id()
    stepfunctions.step_functions_invoke(state_machine_name_template, execution_id, state)
    return execution_id

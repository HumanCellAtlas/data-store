import logging

from . import _awsimpl, awsconstants
from .runner import Runner

# this is the authoritative mapping between client names and Task classes.
CLIENTS = {
    _awsimpl.AWS_FAST_TEST_CLIENT_NAME: _awsimpl.AWSFastTestTask,
}

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def dispatch(context, payload):
    # look up by client name
    try:
        client_name = payload[awsconstants.CLIENT_KEY]
        client_class = CLIENTS[client_name]
        state = payload[awsconstants.STATE_KEY]
    except KeyError as ex:
        logger.error(f"Could not resolve payload {payload} exc {ex}")
        # TODO: clean up logging.
        return

    # special case: if the client name is `AWS_FAST_TEST_CLIENT_NAME`, we use a special runtime environment so we don't
    # take forever running the test.
    if client_name == _awsimpl.AWS_FAST_TEST_CLIENT_NAME:
        runtime = _awsimpl.AWSFastTestRuntime(context)
    else:
        runtime = _awsimpl.AWSRuntime(context, client_name)

    task = client_class(state)

    runner = Runner(task, runtime)
    runner.run()

import json
import os
import typing
import logging

from dss.util.aws import ARN
from dss.util.aws.clients import stepfunctions  # type: ignore
from dss.util.aws import send_sns_msg

"""
The keys used to transfer step function invokation data over SNS to the dss-sfn-* Lambda. dss-sfn starts step function
execution and configured with DLQ for resiliency
"""
SFN_TEMPLATE_KEY = 'sfn_template'
SFN_EXECUTION_KEY = 'sfn_execution'
SFN_INPUT_KEY = 'sfn_input'

region = ARN.get_region()
stage = os.environ["DSS_DEPLOYMENT_STAGE"]
accountid = ARN.get_account_id()

sfn_sns_topic = f"dss-sfn-{stage}"
sfn_sns_topic_arn = f"arn:aws:sns:{region}:{accountid}:{sfn_sns_topic}"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def step_functions_arn(state_machine_name_template: str) -> str:
    """
    The ARN of a state machine, with name derived from `state_machine_name_template`, with string formatting to
    replace {stage} with the dss deployment stage.
    """

    sfn_name = state_machine_name_template.format(stage=stage)
    state_machine_arn = f"arn:aws:states:{region}:{accountid}:stateMachine:{sfn_name}"
    return state_machine_arn


def step_functions_execution_arn(state_machine_name_template: str, execution_name: str) -> str:
    """
    The ARN of a state machine execution, with name derived from `state_machine_name_template`, with string formatting
    to replace {stage} with the dss deployment stage.
    """

    sfn_name = state_machine_name_template.format(stage=stage)
    state_machine_execution_arn = f"arn:aws:states:{region}:{accountid}:execution:{sfn_name}:{execution_name}"
    return state_machine_execution_arn


def step_functions_invoke(state_machine_name_template: str, execution_name: str, input, attributes = None) -> typing.Any:
    """
    Invoke a step functions state machine.  The name of the state machine to be invoked will be derived from
    `state_machine_name_template`, with string formatting to replace {stage} with the dss deployment stage.
    """

    message = {
        SFN_TEMPLATE_KEY: state_machine_name_template,
        SFN_EXECUTION_KEY: execution_name,
        SFN_INPUT_KEY: json.dumps(input)
    }
    logger.debug('Sending message: ' + str(message))

    response = send_sns_msg(sfn_sns_topic_arn, message, attributes)

    return response

def _step_functions_start_execution(state_machine_name_template: str, execution_name: str, execution_input) -> typing.Any:
    """
    Invoke a step functions state machine.  The name of the state machine to be invoked will be derived from
    `state_machine_name_template`, with string formatting to replace {stage} with the dss deployment stage.
    """

    state_machine_arn = step_functions_arn(state_machine_name_template)

    response = stepfunctions.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=execution_input
    )

    return response


def step_functions_describe_execution(state_machine_name_template: str, execution_name: str) -> typing.Any:
    """
    Return description of a step function execution, possible in-progress, completed, errored, etc.
    """
    execution_arn = step_functions_execution_arn(state_machine_name_template, execution_name)
    resp = stepfunctions.describe_execution(executionArn=execution_arn)
    return resp


def step_functions_list_executions(
    state_machine_name_template: str,
    status_filter: str=None,
    max_results_per_page: int=None
) -> typing.Iterable:
    """
    List step function executions, performing paging in the background.
    """

    state_machine_arn = step_functions_arn(state_machine_name_template)

    kwargs = dict(stateMachineArn=state_machine_arn)  # type: typing.Dict[str, typing.Any]

    if max_results_per_page is not None:
        kwargs['maxResults'] = max_results_per_page

    if status_filter is not None:
        kwargs['statusFilter'] = status_filter

    paginator = stepfunctions.get_paginator('list_executions')
    page_iterator = paginator.paginate(**kwargs)

    for page in page_iterator:
        for ex in page['executions']:
            yield ex

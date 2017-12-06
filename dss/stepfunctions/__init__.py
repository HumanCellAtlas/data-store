import json
import os
import typing
import datetime

import boto3


def step_functions_arn(state_machine_name_template: str) -> str:
    """
    The ARN of a state machine, with name derived from `state_machine_name_template`, with string formatting to
    replace {stage} with the dss deployment stage.
    :param state_machine_name_template:
    :return:
    """

    sts_client = boto3.client("sts")

    region = boto3.Session().region_name
    accountid = sts_client.get_caller_identity()['Account']
    stage = os.environ["DSS_DEPLOYMENT_STAGE"]
    sfn_name = state_machine_name_template.format(stage=stage)
    state_machine_arn = f"arn:aws:states:{region}:{accountid}:stateMachine:{sfn_name}"
    return state_machine_arn


def step_functions_execution_arn(state_machine_name_template: str, execution_name: str) -> str:
    """
    The ARN of a state machine execution, with name derived from `state_machine_name_template`, with string formatting
    to replace {stage} with the dss deployment stage.
    :param state_machine_name_template:
    :param execution_name:
    :return:
    """

    sts_client = boto3.client("sts")

    region = boto3.Session().region_name
    accountid = sts_client.get_caller_identity()['Account']
    stage = os.environ["DSS_DEPLOYMENT_STAGE"]
    sfn_name = state_machine_name_template.format(stage=stage)
    state_machine_execution_arn = f"arn:aws:states:{region}:{accountid}:execution:{sfn_name}:{execution_name}"
    return state_machine_execution_arn


def step_functions_invoke(state_machine_name_template: str, execution_name: str, input) -> typing.Any:
    """
    Invoke a step functions state machine.  The name of the state machine to be invoked will be derived from
    `state_machine_name_template`, with string formatting to replace {stage} with the dss deployment stage.
    :param state_machine_name_template:
    :param execution_name:
    :param input:
    :return:
    """

    sfn = boto3.client('stepfunctions')

    execution_input = json.dumps(input)
    state_machine_arn = step_functions_arn(state_machine_name_template)

    response = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=execution_input
    )
    return response


def step_functions_describe_execution(state_machine_name_template: str, execution_name: str) -> typing.Any:
    """
    Return description of a step function exectuion, possible in-progress, completed, errored, etc.
    :param state_machine_name_template:
    :param execution_name:
    :return:
    """

    sfn = boto3.client('stepfunctions')

    execution_arn = step_functions_execution_arn(state_machine_name_template, execution_name)

    resp = sfn.describe_execution(
        executionArn=execution_arn
    )

    return resp


def step_functions_list_executions(state_machine_name_template: str, k_results_per_page: int=100) -> typing.Iterable:
    """
    List step function executions, peforming paging in the background.
    Maximum 100 results per page.
    :param state_machine_name_template:
    :param k_results_per_page:
    :return:
    """

    sfn = boto3.client('stepfunctions')

    state_machine_arn = step_functions_arn(state_machine_name_template)

    kwargs = {
        'stateMachineArn': state_machine_arn,
        'maxResults': k_results_per_page
    }

    while True:
        resp = sfn.list_executions(**kwargs)

        for e in resp['executions']:
            yield e

        if resp.get('nextToken', False):
            kwargs['nextToken'] = resp['nextToken']
        else:
            break

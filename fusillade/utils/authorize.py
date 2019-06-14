import functools
import json
import logging
from typing import List, Dict, Optional, Any

from dcplib.aws import clients as aws_clients

from fusillade import User
from fusillade.errors import FusilladeForbiddenException

logger = logging.getLogger(__name__)
iam = aws_clients.iam


def evaluate_policy(
        principal: str,
        actions: List[str],
        resources: List[str],
        policies: List[str],
        context_entries: List[Dict] = []
) -> bool:
    logger.debug(dict(policies=policies))
    response = iam.simulate_custom_policy(
        PolicyInputList=policies,
        ActionNames=actions,
        ResourceArns=resources,
        ContextEntries=[
            {
                'ContextKeyName': 'fus:user_email',
                'ContextKeyValues': [principal],
                'ContextKeyType': 'string'
            }, *context_entries
        ]
    )
    logger.debug(json.dumps(response))
    results = [result['EvalDecision'] for result in response['EvaluationResults']]
    if 'explicitDeny' in results:
        return False
    elif 'allowed' in results:
        return True
    else:
        return False


def assert_authorized(user, actions, resources, context_entries=None):
    """
    Asserts a user has permission to perform actions on resources.

    :param user:
    :param actions:
    :param resources:
    :param context_entries:
    :return:
    """
    u = User(user)
    policies = u.lookup_policies()
    _context_entries = [
        {
            'ContextKeyName': key,
            'ContextKeyValues': value if isinstance(value, list) else [value],
            'ContextKeyType': 'string'
        } for key, value in context_entries.items()] if context_entries else []
    if not evaluate_policy(user, actions, resources, policies, _context_entries):
        logger.info(dict(message="User not authorized.", user=u._path_name, action=actions, resources=resources))
        raise FusilladeForbiddenException()
    else:
        logger.info(dict(message="User authorized.", user=u._path_name, action=actions,
                         resources=resources))


def format_resources(resources: List[str], resource_param: List[str], kwargs: Dict[str, Any]):
    """
    >>> resources=['hello/{user_name}']
    >>> resource_param=['user_name']
    >>> kwargs={'user_name': "bob"}
    >>> x = format_resources(resources, resource_param, kwargs)
    >>> x == ['hello/bob']

    :param resources:
    :param resource_param:
    :param kwargs:
    :return:
    """
    _rp = dict()
    for key in resource_param:
        v = kwargs.get(key)
        if isinstance(v, str):
            _rp[key] = v
    return [resource.format_map(_rp) for resource in resources]


def format_context_entries(context_entries, kwargs):
    """
    >>> context_entries={"fus:context": "user_name"}
    >>> kwargs={'user_name': "bob"}
    >>> x = format_resources(context_entries, kwargs)
    >>> x == {"fus:context": "bob"}

    :param context_entries:
    :param kwargs:
    :return:
    """
    _ce = dict()
    for key, value in context_entries.items():
        if isinstance(value, list):
            temp = []
            for i in value:
                v = kwargs.get(i)
                if isinstance(v, str):
                    temp.append(v)
            _ce[key] = temp
        else:
            v = kwargs.get(value)
            if isinstance(v, str):
                _ce[key] = v
    return _ce


def authorize(actions: List[str],
              resources: List[str],
              resource_params: Optional[List[str]] = None,
              context_entries: Optional[List[str]] = None
              ):
    """
    A decorator for assert_authorized

    :param actions: The actions passed to assert_authorized
    :param resources: The resources passed to assert_authorized
    :param resource_params: Keys to extract from kwargs and map into the resource strings.
    :return:
    """

    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
            assert_authorized(kwargs['token_info']['https://auth.data.humancellatlas.org/email'],
                              actions,
                              format_resources(resources, resource_params, kwargs) if resource_params else resources,
                              format_context_entries(context_entries, kwargs) if context_entries else None
                              )
            return func(*args, **kwargs)

        return call

    return decorate

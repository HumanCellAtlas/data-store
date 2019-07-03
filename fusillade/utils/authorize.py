import functools
import json
import logging
from typing import List, Dict, Optional, Union

from dcplib.aws import clients as aws_clients

from fusillade import User
from fusillade.errors import FusilladeForbiddenException, AuthorizationException, FusilladeBadRequestException

logger = logging.getLogger(__name__)
iam = aws_clients.iam


def evaluate_policy(
        principal: str,
        actions: List[str],
        resources: List[str],
        policies: List[str],
        context_entries: List[Dict] = None
) -> bool:
    logger.debug(dict(policies=policies))
    context_entries = context_entries if context_entries else []
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
    context_entries = context_entries if context_entries else []
    try:
        authz_params = u.get_authz_params()
    except AuthorizationException:
        raise FusilladeForbiddenException(detail="User must be enabled to make authenticated requests.")
    else:
        context_entries.extend(restricted_context_entries(authz_params))
        if not evaluate_policy(user, actions, resources, authz_params['policies'], context_entries):
            logger.info(dict(message="User not authorized.", user=u._path_name, action=actions, resources=resources))
            raise FusilladeForbiddenException()
        else:
            logger.info(dict(message="User authorized.", user=u._path_name, action=actions,
                             resources=resources))


restricted_set = {'fus:groups', 'fus:roles', 'fus:user_email'}


def restricted_context_entries(authz_params):
    """
    Restricts context_entries from containing reserved entries.

    :param authz_params:
    :return:
    """
    try:
        return format_context_entries({'fus:groups': 'group', 'fus:roles': 'role'}, authz_params)
    except KeyError:
        FusilladeBadRequestException("Invalid context entry type.")


def format_resources(resources: List[str], resource_param: List[str], kwargs: Dict[str, Union[List[str], str]]):
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


context_type_mapping = {
    str: 'string',
    # int: 'numeric',  # TODO: add support
    # float: 'numeric',  # TODO: add support
    # bool: 'bool'  # TODO: add support
}


def format_context_entries(context_entries: Dict[str, str],
                           kwargs: Dict[str, Union[List[str], str]],
                           restricted: List[str] = None) -> List[Dict]:
    """
    >>> context_entries={"fus:context": "user_name", "fus:groups": "group"}
    >>> kwargs={'user_name': "bob", 'group': ['g1', 'g2']}
    >>> x = format_resources(context_entries, kwargs)
    >>> x == [{"ContextKeyName": "fus:context","ContextKeyValues":["bob"],"ContextKeyType": "string"},
    >>>       {"ContextKeyName": "fus:groups","ContextKeyValues":["g1", "g2"],"ContextKeyType": "stringList"}]

    :param context_entries:
    :param kwargs:
    :param restricted: A list of context variables not allowed.
    :return:
    """
    _ce = []
    restricted = restricted if restricted else []
    for key, value in context_entries.items():
        if key not in restricted:
            v = kwargs.get(value)
            if v is None:
                continue
            elif isinstance(v, list) and v:
                v_type = type(v[0])
                if all(isinstance(i, v_type) for i in v):
                    suffix = "List"
            else:
                v_type = type(v)
                v = [v]
                suffix = ""
            _ce.append({
                'ContextKeyName': key,
                'ContextKeyValues': v,
                'ContextKeyType': f"{context_type_mapping[v_type]}{suffix}"
            })
    return _ce


def authorize(actions: List[str],
              resources: List[str],
              resource_params: Optional[List[str]] = None,
              context_entries: Optional[Dict[str, str]] = None
              ):
    """
    A decorator for assert_authorized

    :param actions: The actions passed to assert_authorized
    :param resources: The resources passed to assert_authorized
    :param resource_params: Keys to extract from kwargs and map into the resource strings.
    :param context_entries: Keys to extract from kwargs and map into iam policy context variables
    :return:
    """

    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
            assert_authorized(kwargs['token_info']['https://auth.data.humancellatlas.org/email'],
                              actions,
                              format_resources(resources, resource_params, kwargs) if resource_params else resources,
                              format_context_entries(context_entries, kwargs,
                                                     restricted_set) if context_entries else None
                              )
            return func(*args, **kwargs)

        return call

    return decorate

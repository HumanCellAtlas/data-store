"""
clouddrectory.py

This modules is used to simplify access to AWS Cloud Directory. For more information on AWS Cloud Directory see
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/clouddirectory.html

"""
import functools
import hashlib
import json
import logging
import os
from collections import namedtuple
from datetime import datetime
from enum import Enum, auto
from typing import Iterator, Any, Tuple, Dict, List, Callable, Optional, Union, Type

from dcplib.aws import clients as aws_clients

from fusillade.errors import FusilladeException, FusilladeHTTPException, FusilladeNotFoundException
from fusillade.utils.retry import retry

logger = logging.getLogger(__name__)

cd_client = aws_clients.clouddirectory
iam = aws_clients.iam
project_arn = "arn:aws:clouddirectory:{}:{}:".format(
    os.getenv('AWS_DEFAULT_REGION'),
    aws_clients.sts.get_caller_identity().get('Account'))
proj_path = os.path.dirname(__file__)

# TODO make all configurable
directory_schema_path = os.path.join(proj_path, 'directory_schema.json')
default_user_policy_path = os.path.join(proj_path, '..', 'policies', 'default_user_policy.json')
default_group_policy_path = os.path.join(proj_path, '..', 'policies', 'default_group_policy.json')
default_admin_role_path = os.path.join(proj_path, '..', 'policies', 'default_admin_role.json')
default_user_role_path = os.path.join(proj_path, '..', 'policies', 'default_user_role.json')
default_role_path = os.path.join(proj_path, '..', 'policies', 'default_role.json')


def get_json_file(file_name):
    with open(file_name, 'r') as fp:
        return json.dumps(json.load(fp))


def get_published_schema_from_directory(dir_arn: str) -> str:
    schema = cd_client.list_applied_schema_arns(DirectoryArn=dir_arn)['SchemaArns'][0]
    schema = schema.split('/')[-2:]
    schema = '/'.join(schema)
    return f"{project_arn}schema/published/{schema}"


def cleanup_directory(dir_arn: str):
    cd_client.disable_directory(DirectoryArn=dir_arn)
    cd_client.delete_directory(DirectoryArn=dir_arn)
    logger.warning({"message": "Deleted directory", "directory_arn": dir_arn})


def cleanup_schema(sch_arn: str) -> None:
    cd_client.delete_schema(SchemaArn=sch_arn)
    logger.warning({"message": "Deleted schema", "schema_arn": sch_arn})


def publish_schema(name: str, version: str) -> str:
    """
    More info about schemas
    https://docs.aws.amazon.com/clouddirectory/latest/developerguide/schemas.html
    """
    # don't create if already created
    try:
        dev_schema_arn = cd_client.create_schema(Name=name)['SchemaArn']
        logger.info({"message": "Created development schema", "developement_schema_arn": dev_schema_arn})
    except cd_client.exceptions.SchemaAlreadyExistsException:
        dev_schema_arn = f"{project_arn}schema/development/{name}"

    # update the schema
    schema = get_json_file(directory_schema_path)
    cd_client.put_schema_from_json(SchemaArn=dev_schema_arn, Document=schema)
    try:
        pub_schema_arn = cd_client.publish_schema(DevelopmentSchemaArn=dev_schema_arn,
                                                  Version=version)['PublishedSchemaArn']
        logger.info({"message": "Published development schema",
                     "developement_schema_arn": dev_schema_arn,
                     "published_schema_arn": pub_schema_arn})
    except cd_client.exceptions.SchemaAlreadyPublishedException:
        pub_schema_arn = f"{project_arn}schema/published/{name}/{version}"
    return pub_schema_arn


def create_directory(name: str, schema: str, admins: List[str]) -> 'CloudDirectory':
    """
    Retrieve the fusillade cloud directory or do a one time setup of cloud directory to be used with fusillade.

    :param name:
    :param schema:
    :param admins: a list of admins to create
    :return:
    """
    directory = None
    try:
        response = cd_client.create_directory(
            Name=name,
            SchemaArn=schema
        )
        directory = CloudDirectory(response['DirectoryArn'])
        logger.info({"message": "Created new directory", "directory_arn": directory._dir_arn})
        cd_client.tag_resource(
            ResourceArn=directory._dir_arn,
            Tags=[
                {'Key': 'project', "Value": os.getenv("FUS_PROJECT_TAG", '')},
                {'Key': 'owner', "Value": os.getenv("FUS_OWNER_TAG", '')},
                {'Key': 'env', "Value": os.getenv("FUS_DEPLOYMENT_STAGE")},
                {'Key': 'Name', "Value": "fusillade-directory"},
                {'Key': 'managedBy', "Value": "manual"}
            ]
        )
    except cd_client.exceptions.DirectoryAlreadyExistsException:
        directory = CloudDirectory.from_name(name)
    else:
        # create structure
        for folder_name in ('group', 'user', 'role', 'policy'):
            directory.create_folder('/', folder_name)

        # create roles
        Role.create(directory, "default_user", statement=get_json_file(default_user_role_path))
        Role.create(directory, "fusillade_admin", statement=get_json_file(default_admin_role_path))
        Group.create(directory, "user_default").add_roles(['default_user'])

        # create admins
        for admin in admins:
            User.provision_user(directory, admin, roles=['fusillade_admin'])
        User.provision_user(directory, 'public')
    finally:
        return directory


def _paging_loop(fn: Callable, key: str, upack_response: Optional[Callable] = None, **kwarg):
    while True:
        resp = fn(**kwarg)
        for i in resp[key]:
            yield i if not upack_response else upack_response(i)
        kwarg['NextToken'] = resp.get("NextToken")
        if not kwarg['NextToken']:
            break


def list_directories(state: str = 'ENABLED') -> Iterator:
    return _paging_loop(cd_client.list_directories, 'Directories', state=state)


class UpdateActions(Enum):
    CREATE_OR_UPDATE = auto()
    DELETE = auto()


class ValueTypes(Enum):
    StringValue = auto()
    BinaryValue = auto()
    BooleanValue = auto()
    NumberValue = auto()
    DatetimeValue = auto()


class UpdateObjectParams(namedtuple("UpdateObjectParams", ['facet', 'attribute', 'value_type', 'value', 'action'])):
    pass


cd_retry_parameters = dict(timeout=.5,
                           delay=0.1,
                           retryable=lambda e: isinstance(e, cd_client.exceptions.RetryableConflictException))


class CloudDirectory:
    _page_limit = 30  # This is the max allowed by AWS
    _batch_write_max = 20  # This is the max allowed by AWS
    _lookup_policy_max = 3  # Max recommended by AWS Support

    def __init__(self, directory_arn: str):
        self._dir_arn = directory_arn
        self._schema = None
        # This is the custom schema applied to the cloud directory. It is defined in fusillade/directory_schema.json.
        self.node_schema = f"{self._dir_arn}/schema/CloudDirectory/1.0"
        # This is the base schema that is always present in AWS Cloud Directory. It defines the basic Node types, NODE,
        # POLICY, LEAF_NODE, and INDEX.

    @classmethod
    @functools.lru_cache()
    def from_name(cls, dir_name: str) -> 'CloudDirectory':
        # get directory arn by name
        for i in list_directories():
            if i['Name'] == dir_name:
                dir_arn = i['DirectoryArn']
                return cls(dir_arn)
        raise FusilladeException(f"{dir_name} does not exist")

    @property
    def schema(self):
        if not self._schema:
            self._schema = cd_client.list_applied_schema_arns(DirectoryArn=self._dir_arn)['SchemaArns'][0]
        return self._schema

    def list_object_children_paged(self, object_ref: str,
                                   next_token: Optional[str] = None,
                                   per_page=None) -> Tuple[dict, Optional[str]]:
        """
        a wrapper around CloudDirectory.Client.list_object_children with paging

        :param object_ref:
        :param next_token:
        :param per_page:
        :return:
        """
        kwargs = dict(
            DirectoryArn=self._dir_arn,
            ObjectReference={'Selector': object_ref},
            ConsistencyLevel='EVENTUAL',
            MaxResults=min(per_page, self._page_limit) if per_page else self._page_limit,
        )
        if next_token:
            kwargs['NextToken'] = next_token
        result = cd_client.list_object_children(**kwargs)
        return result['Children'], result.get("NextToken")

    def list_object_children(self, object_ref: str) -> Iterator[Tuple[str, str]]:
        """
        a wrapper around CloudDirectory.Client.list_object_children
        """
        resp = cd_client.list_object_children(DirectoryArn=self._dir_arn,
                                              ObjectReference={'Selector': object_ref},
                                              ConsistencyLevel='EVENTUAL',
                                              MaxResults=self._page_limit)
        while True:
            for name, ref in resp['Children'].items():
                yield name, '$' + ref
            next_token = resp.get('NextToken')
            if next_token:
                resp = cd_client.list_object_children(DirectoryArn=self._dir_arn,
                                                      ObjectReference={'Selector': object_ref},
                                                      NextToken=next_token,
                                                      MaxResults=self._page_limit)
            else:
                break

    def list_object_parents(self,
                            object_ref: str,
                            include_all_links_to_each_parent: bool = True) -> Iterator:
        """
        a wrapper around CloudDirectory.Client.list_object_parents with paging
        """
        if include_all_links_to_each_parent:
            def unpack_response(i):
                return '$' + i['ObjectIdentifier'], i['LinkName']

            return _paging_loop(cd_client.list_object_parents,
                                'ParentLinks',
                                unpack_response,
                                DirectoryArn=self._dir_arn,
                                ObjectReference={'Selector': object_ref},
                                ConsistencyLevel='EVENTUAL',
                                IncludeAllLinksToEachParent=include_all_links_to_each_parent,
                                MaxResults=self._page_limit
                                )
        else:
            return _paging_loop(cd_client.list_object_parents,
                                'Parents',
                                self._make_ref,
                                DirectoryArn=self._dir_arn,
                                ObjectReference={'Selector': object_ref},
                                ConsistencyLevel='EVENTUAL',
                                IncludeAllLinksToEachParent=include_all_links_to_each_parent,
                                MaxResults=self._page_limit
                                )

    def list_object_policies(self, object_ref: str) -> Iterator[str]:
        """
        a wrapper around CloudDirectory.Client.list_object_policies with paging
        """
        return _paging_loop(cd_client.list_object_policies,
                            'AttachedPolicyIds',
                            self._make_ref,
                            DirectoryArn=self._dir_arn,
                            ObjectReference={'Selector': object_ref},
                            MaxResults=self._page_limit
                            )

    def list_policy_attachments(self, policy: str) -> Iterator[str]:
        """
        a wrapper around CloudDirectory.Client.list_policy_attachments with paging
        """
        return _paging_loop(cd_client.list_policy_attachments,
                            'ObjectIdentifiers',
                            self._make_ref,
                            DirectoryArn=self._dir_arn,
                            PolicyReference={'Selector': policy},
                            MaxResults=self._page_limit
                            )

    def _list_typed_links(self,
                          func: Callable,
                          key: str,
                          object_ref: str,
                          filter_attribute_ranges: Optional[List],
                          filter_typed_link: Optional[str],
                          paged=False,
                          per_page=None,
                          next_token=None,
                          **kwargs) -> Union[Iterator[dict], Tuple[List[Dict], str]]:
        kwargs.update(dict(
            DirectoryArn=self._dir_arn,
            ObjectReference={'Selector': object_ref},
            MaxResults=min(per_page, self._page_limit) if per_page else self._page_limit
        ))
        if filter_attribute_ranges:
            kwargs['FilterAttributeRanges'] = filter_attribute_ranges
        if filter_typed_link:
            kwargs['FilterTypedLink'] = {
                'SchemaArn': self.schema,
                'TypedLinkName': filter_typed_link
            }
        if next_token:
            kwargs["NextToken"] = next_token
        if paged:
            resp = func(**kwargs)
            return [i for i in resp[key]], resp.get("NextToken")
        else:
            return _paging_loop(func, key, **kwargs)

    def list_outgoing_typed_links(self,
                                  object_ref: str,
                                  filter_attribute_ranges: List = None,
                                  filter_typed_link: str = None,
                                  **kwargs) -> Iterator[dict]:
        """
        a wrapper around CloudDirectory.Client.list_outgoing_typed_links

        :return: typed link specifier generator
        """
        return self._list_typed_links(cd_client.list_outgoing_typed_links,
                                      'TypedLinkSpecifiers',
                                      object_ref,
                                      filter_attribute_ranges,
                                      filter_typed_link,
                                      **kwargs)

    def list_incoming_typed_links(
            self,
            object_ref: str,
            filter_attribute_ranges: List = None,
            filter_typed_link: str = None,
            **kwargs) -> Iterator[dict]:
        """
        a wrapper around CloudDirectory.Client.list_incoming_typed_links

        :return: typed link specifier generator
        """
        return self._list_typed_links(
            cd_client.list_incoming_typed_links,
            'LinkSpecifiers',
            object_ref,
            filter_attribute_ranges,
            filter_typed_link,
            **kwargs)

    @staticmethod
    def _make_ref(i):
        return '$' + i

    def create_object(self, link_name: str, facet_type: str, obj_type: str, **kwargs) -> str:
        """
        Create an object and store in cloud directory.
        """
        object_attribute_list = self.get_object_attribute_list(facet=facet_type, obj_type=obj_type, **kwargs)
        parent_path = self.get_obj_type_path(obj_type)
        cd_client.create_object(DirectoryArn=self._dir_arn,
                                SchemaFacets=[
                                    {
                                        'SchemaArn': self.schema,
                                        'FacetName': facet_type
                                    },
                                ],
                                ObjectAttributeList=object_attribute_list,
                                ParentReference=dict(Selector=parent_path),
                                LinkName=link_name)
        object_ref = parent_path + link_name
        return object_ref

    def get_object_attributes(self, obj_ref: str, facet: str, attributes: List[str],
                              schema=None) -> Dict[str, Any]:
        """
        a wrapper around CloudDirectory.Client.get_object_attributes
        """
        if not schema:
            schema = self.schema
        return cd_client.get_object_attributes(DirectoryArn=self._dir_arn,
                                               ObjectReference={'Selector': obj_ref},
                                               SchemaFacet={
                                                   'SchemaArn': schema,
                                                   'FacetName': facet
                                               },
                                               AttributeNames=attributes
                                               )

    def get_object_attribute_list(self, facet="LeafFacet", **kwargs) -> List[Dict[str, Any]]:
        return [dict(Key=dict(SchemaArn=self.schema, FacetName=facet, Name=k), Value=dict(StringValue=v))
                for k, v in kwargs.items()]

    def get_policy_attribute_list(self,
                                  policy_type: str,
                                  statement: str,
                                  **kwargs) -> List[Dict[str, Any]]:
        """
        policy_type and policy_document are required field for a policy object. See the section on Policies for more
        info https://docs.aws.amazon.com/clouddirectory/latest/developerguide/key_concepts_directory.html
        """
        attributes = self.get_object_attribute_list(facet='IAMPolicy', **kwargs)
        attributes.extend([
            dict(
                Key=dict(
                    SchemaArn=self.node_schema,
                    FacetName='POLICY',
                    Name='policy_type'),
                Value=dict(
                    StringValue=policy_type)),
            dict(
                Key=dict(
                    SchemaArn=self.node_schema,
                    FacetName='POLICY',
                    Name="policy_document"),
                Value=dict(
                    BinaryValue=statement.encode()))
        ])
        return attributes

    def update_object_attribute(self,
                                object_ref: str,
                                update_params: List[UpdateObjectParams],
                                schema=None) -> Dict[str, Any]:
        """
        a wrapper around CloudDirectory.Client.update_object_attributes

        :param object_ref: The reference that identifies the object.
        :param update_params: a list of attributes to modify.
        :param schema:
        :return:
        """
        if not schema:
            schema = self.schema
        updates = [
            {
                'ObjectAttributeKey': {
                    'SchemaArn': schema,
                    'FacetName': i.facet,
                    'Name': i.attribute
                },
                'ObjectAttributeAction': {
                    'ObjectAttributeActionType': i.action.name,
                    'ObjectAttributeUpdateValue': {
                        i.value_type.name: i.value
                    }
                }
            } for i in update_params]
        return cd_client.update_object_attributes(
            DirectoryArn=self._dir_arn,
            ObjectReference={
                'Selector': object_ref
            },
            AttributeUpdates=updates
        )

    def create_folder(self, path: str, name: str) -> None:
        """ A folder is just a NodeFacet"""
        schema_facets = [dict(SchemaArn=self.schema, FacetName="NodeFacet")]
        object_attribute_list = self.get_object_attribute_list(facet="NodeFacet", name=name, obj_type="folder",
                                                               created_by="fusillade")
        try:
            cd_client.create_object(DirectoryArn=self._dir_arn,
                                    SchemaFacets=schema_facets,
                                    ObjectAttributeList=object_attribute_list,
                                    ParentReference=dict(Selector=path),
                                    LinkName=name)
            logger.info({"message": "creating folder", "name": name, "path": path})
        except cd_client.exceptions.LinkNameAlreadyInUseException:
            pass

    def attach_typed_link(
            self,
            source: str,
            target: str,
            typed_link_facet: str,
            attributes: Dict[str, Any]):
        """
        a wrapper around CloudDirectory.Client.attach_typed_link
        """
        return cd_client.attach_typed_link(
            DirectoryArn=self._dir_arn,
            SourceObjectReference={
                'Selector': source
            },
            TargetObjectReference={
                'Selector': target
            },
            TypedLinkFacet={
                'SchemaArn': self.schema,
                'TypedLinkName': typed_link_facet
            },
            Attributes=self.make_attributes(attributes)
        )

    def detach_typed_link(self, typed_link_specifier: Dict[str, Any]):
        """
        a wrapper around CloudDirectory.Client.detach_typed_link

        :param typed_link_specifier: identifies the typed link to remove
        :return:
        """
        return cd_client.detach_typed_link(
            DirectoryArn=self._dir_arn,
            TypedLinkSpecifier=typed_link_specifier
        )

    @staticmethod
    def make_attributes(kwargs: Dict[str, Any]) -> List:
        """
        A helper function used to create
        :param kwargs:
        :return:
        """

        def _make_attribute(name: str, value: any):
            attribute = {'AttributeName': name}
            if isinstance(value, str):
                attribute['Value'] = {ValueTypes.StringValue.name: value}
            elif isinstance(value, bytes):
                attribute['Value'] = {ValueTypes.BinaryValue.name: value}
            elif isinstance(value, bool):
                attribute['Value'] = {ValueTypes.BooleanValue.name: value}
            elif isinstance(value, int):
                attribute['Value'] = {ValueTypes.NumberValue.name: str(value)}
                #  int to str is required by cloud directory
            elif isinstance(value, datetime):
                attribute['Value'] = {ValueTypes.DatetimeValue.name: value}
            else:
                raise ValueError()
            return attribute

        return [_make_attribute(name, value) for name, value in kwargs.items()]

    def make_typed_link_specifier(
            self,
            source_object_ref: str,
            target_object_ref: str,
            typed_link_facet_name: str,
            attributes: Dict[str, Any]):
        return {
            'SourceObjectReference': {
                'Selector': source_object_ref
            },
            'TargetObjectReference': {
                'Selector': target_object_ref
            },
            'TypedLinkFacet': {
                'SchemaArn': self.schema,
                'TypedLinkName': typed_link_facet_name
            },
            'IdentityAttributeValues': self.make_attributes(attributes)
        }

    @retry(**cd_retry_parameters)
    def clear(self, users: List[str] = None,
              groups: List[str] = None,
              roles: List[str] = None) -> None:
        """

        :param users: a list of users to keep
        :param groups: a list of groups to keep
        :param roles: a list of roles to keep
        :return:
        """
        users = users if users else []
        groups = groups if groups else []
        roles = roles if roles else []
        protected_users = [CloudNode.hash_name(name) for name in ['public'] + users]
        protected_groups = [CloudNode.hash_name(name) for name in ['user_default'] + groups]
        protected_roles = [CloudNode.hash_name(name) for name in ["fusillade_admin", "default_user"] + roles]

        for name, obj_ref in self.list_object_children('/user/'):
            if name not in protected_users:
                self.delete_object(obj_ref)
        for name, obj_ref in self.list_object_children('/group/'):
            if name not in protected_groups:
                self.delete_object(obj_ref)
        for name, obj_ref in self.list_object_children('/role/'):
            if name not in protected_roles:
                self.delete_object(obj_ref)

    def delete_policy(self, policy_ref: str) -> None:
        """
        See details on deletion requirements for more info
        https://docs.aws.amazon.com/clouddirectory/latest/developerguide/directory_objects_access_objects.html
        """
        self.batch_write([self.batch_detach_policy(policy_ref, obj_ref)
                          for obj_ref in self.list_policy_attachments(policy_ref)])
        self.batch_write([self.batch_detach_object(parent_ref, link_name)
                          for parent_ref, link_name in self.list_object_parents(policy_ref)])
        cd_client.delete_object(DirectoryArn=self._dir_arn, ObjectReference={'Selector': policy_ref})

    def delete_object(self, obj_ref: str) -> None:
        """
        See details on deletion requirements for more info
        https://docs.aws.amazon.com/clouddirectory/latest/developerguide/directory_objects_access_objects.html
        """
        [self.delete_policy(policy_ref) for policy_ref in self.list_object_policies(obj_ref)]
        self.batch_write([self.batch_detach_object(parent_ref, link_name)
                          for parent_ref, link_name in self.list_object_parents(obj_ref)])
        self.batch_write([self.batch_detach_typed_link(i) for i in self.list_incoming_typed_links(object_ref=obj_ref)])
        self.batch_write([self.batch_detach_typed_link(i) for i in self.list_outgoing_typed_links(obj_ref)])
        cd_client.delete_object(DirectoryArn=self._dir_arn, ObjectReference={'Selector': obj_ref})

    @staticmethod
    def batch_detach_policy(policy_ref: str, object_ref: str):
        """
        A helper function to format a batch detach_policy operation
        """
        return {
            'DetachPolicy': {
                'PolicyReference': {'Selector': policy_ref},
                'ObjectReference': {'Selector': object_ref}
            }
        }

    def batch_create_object(self, parent: str,
                            name: str,
                            facet_name: str,
                            object_attribute_list: List[str]) -> Dict[str, Any]:
        """
        A helper function to format a batch create_object operation
        """
        return {'CreateObject': {
            'SchemaFacet': [
                {
                    'SchemaArn': self.schema,
                    'FacetName': facet_name
                },
            ],
            'ObjectAttributeList': object_attribute_list,
            'ParentReference': {
                'Selector': parent
            },
            'LinkName': name,
        }
        }

    def batch_get_attributes(self, obj_ref, facet, attributes: List[str], schema=None) -> Dict[str, Any]:
        """
        A helper function to format a batch get_attributes operation
        """
        return {
            'GetObjectAttributes': {
                'ObjectReference': {
                    'Selector': obj_ref
                },
                'SchemaFacet': {
                    'SchemaArn': schema if schema else self.schema,
                    'FacetName': facet
                },
                'AttributeNames': attributes
            }
        }

    @staticmethod
    def batch_attach_object(parent: str, child: str, name: str) -> Dict[str, Any]:
        """
        A helper function to format a batch attach_object operation
        """
        return {
            'AttachObject': {
                'ParentReference': {
                    'Selector': parent
                },
                'ChildReference': {
                    'Selector': child
                },
                'LinkName': name
            }
        }

    @staticmethod
    def batch_detach_object(parent: str, link_name: str) -> Dict[str, Any]:
        """
        A helper function to format a batch detach_object operation
        """
        return {'DetachObject': {
            'ParentReference': {
                'Selector': parent
            },
            'LinkName': link_name,
        }}

    @staticmethod
    def batch_attach_policy(policy: str, object_ref: str) -> Dict[str, Any]:
        """
        A helper function to format a batch attach_policy operation
        """
        return {
            'AttachPolicy': {
                'PolicyReference': {
                    'Selector': policy
                },
                'ObjectReference': {
                    'Selector': object_ref
                }
            }
        }

    def batch_attach_typed_link(self,
                                parent: str,
                                child: str,
                                facet_name: str,
                                attributes: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'AttachTypedLink': {
                'SourceObjectReference': {
                    'Selector': parent
                },
                'TargetObjectReference': {
                    'Selector': child
                },
                'TypedLinkFacet': {
                    'SchemaArn': self.schema,
                    'TypedLinkName': facet_name
                },
                'Attributes': self.make_attributes(attributes)
            }
        }

    @staticmethod
    def batch_detach_typed_link(typed_link_specifier) -> Dict[str, Any]:
        return {
            'DetachTypedLink': {
                'TypedLinkSpecifier': typed_link_specifier
            },
        }

    def batch_lookup_policy(self, obj_ref: str, next_token: str = None) -> Dict[str, Any]:
        temp = {
            'ObjectReference': {
                'Selector': obj_ref
            },
            'MaxResults': self._lookup_policy_max
        }
        if next_token:
            temp['NextToken'] = next_token
        return {'LookupPolicy': temp}

    @retry(**cd_retry_parameters)
    def batch_write(self, operations: list) -> List[dict]:
        """
        A wrapper around CloudDirectory.Client.batch_write
        """
        responses = []
        for i in range(0, len(operations), self._batch_write_max):
            responses.extend(
                cd_client.batch_write(
                    DirectoryArn=self._dir_arn,
                    Operations=operations[i:i + self._batch_write_max])['Responses'])
        return responses

    @retry(**cd_retry_parameters)
    def batch_read(self, operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        A wrapper around CloudDirectory.Client.batch_read
        """
        return cd_client.batch_read(DirectoryArn=self._dir_arn, Operations=operations)

    @staticmethod
    def get_obj_type_path(obj_type: str) -> str:
        obj_type = obj_type.lower()
        paths = dict(group='/group/',
                     index='/index/',
                     user='/user/',
                     policy='/policy/',
                     role='/role/')
        return paths[obj_type]

    def lookup_policy(self, object_id: str) -> List[Dict[str, Any]]:
        # retrieve all of the policies attached to an object and its parents.
        policies_paths = [
            path
            for response in cd_client.get_paginator('lookup_policy').paginate(
                DirectoryArn=self._dir_arn,
                ObjectReference={'Selector': object_id},
                PaginationConfig={'PageSize': self._lookup_policy_max}
            )
            for path in response['PolicyToPathList']
        ]
        return policies_paths

    def get_link_attributes(self, TypedLinkSpecifier, AttributeNames):
        cd_client.get_link_attributes(
            DirectoryArn=self._dir_arn,
            TypedLinkSpecifier=TypedLinkSpecifier,
            AttributeNames=AttributeNames,
            ConsistencyLevel='EVENTUAL'
        )

    def get_policies(self, policy_paths: List[Dict[str, Any]], policy_type='IAMPolicy') -> List[str]:
        # Parse the policyIds from the policies path. Only keep the unique ids
        policy_ids = set(
            [
                o['PolicyId']
                for p in policy_paths
                for o in p['Policies']
                if o.get('PolicyId') and o['PolicyType'] == policy_type
            ]
        )

        # retrieve the policies in a single request
        operations = [
            {
                'GetObjectAttributes': {
                    'ObjectReference': {'Selector': f'${policy_id}'},
                    'SchemaFacet': {
                        'SchemaArn': self.node_schema,
                        'FacetName': 'POLICY'
                    },
                    'AttributeNames': ['policy_document']
                }
            }
            for policy_id in policy_ids
        ]

        # parse the policies from the responses
        policies = [
            response['SuccessfulResponse']['GetObjectAttributes']['Attributes'][0]['Value']['BinaryValue'].decode(
                'utf-8')
            for response in cd_client.batch_read(DirectoryArn=self._dir_arn, Operations=operations)['Responses']
        ]
        return policies

    def get_object_information(self, obj_ref: str) -> Dict[str, Any]:
        """
        A wrapper around CloudDirectory.Client.get_object_information
        """
        return cd_client.get_object_information(
            DirectoryArn=self._dir_arn,
            ObjectReference={
                'Selector': obj_ref
            },
            ConsistencyLevel='EVENTUAL'
        )

    def get_health_status(self) -> dict:
        """
        Runs a health check on AWS cloud directory and iam policy simulator
        :return: the status of the services.
        """
        health_status = {}
        try:
            iam.simulate_custom_policy(
                PolicyInputList=[json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Sid": "DefaultRole",
                        "Effect": "Deny",
                        "Action": ["fake:action"],
                        "Resource": "fake:resource"
                    }]})],
                ActionNames=["fake:action"],
                ResourceArns=["arn:aws:iam::123456789012:user/Bob"])
        except Exception:
            health_status.update(iam_health_status='unhealthy')
        else:
            health_status.update(iam_health_status='ok')

        try:
            self.get_object_information('/')['ResponseMetadata']['HTTPStatusCode']
        except Exception:
            health_status.update(clouddirectory_health_status='unhealthy')
        else:
            health_status.update(clouddirectory_health_status='ok')
        return health_status


class CloudNode:
    """
    Contains shared code across the different types of nodes stored in Fusillade CloudDirectory
    """
    _attributes = ["name"]  # the different attributes of a node stored
    _facet = 'LeafNode'
    object_type = 'node'
    allowed_policy_types = ('IAMPolicy',)

    def __init__(self,
                 cloud_directory: CloudDirectory,
                 name: str = None,
                 object_ref: str = None):
        """

        :param cloud_directory:
        :param name:
        :param object_ref:
        """
        if name and object_ref:
            raise FusilladeException("object_reference XOR name")
        if name:
            self._name: str = name
            self._path_name: str = self.hash_name(name)
            self.object_ref: str = cloud_directory.get_obj_type_path(self.object_type) + self._path_name
        else:
            self._name: str = None
            self._path_name: str = None
            self.object_ref: str = object_ref
        self.cd: CloudDirectory = cloud_directory
        self.attached_policies: Dict[str, str] = dict()

    @staticmethod
    def hash_name(name):
        """Generate the cloud directory path name from the nodes name."""
        return hashlib.sha1(bytes(name, "utf-8")).hexdigest()

    def _get_link_name(self, parent_path: str, child_path: str):
        return self.hash_name(parent_path + child_path)
        # links names must be unique between two objects

    def _get_links(self, node: Type['CloudNode'],
                   attribute_name,
                   attribute_value,
                   facet,
                   next_token=None,
                   per_page=None,
                   paged=False,
                   incoming=False):
        """
        Retrieves the links attached to this object from CloudDirectory and separates them into groups and roles
        based on the link name
        """
        get_links = self.cd.list_incoming_typed_links if incoming else self.cd.list_outgoing_typed_links
        object_selection = 'SourceObjectReference' if incoming else 'TargetObjectReference'
        filter_attribute_ranges = [
            {
                'AttributeName': attribute_name,
                'Range': {
                    'StartMode': 'INCLUSIVE',
                    'StartValue': {'StringValue': attribute_value},
                    'EndMode': 'INCLUSIVE',
                    'EndValue': {'StringValue': attribute_value}
                }
            }
        ]
        if paged:
            result, next_token = get_links(self.object_ref, filter_attribute_ranges, facet,
                                           next_token=next_token, paged=paged, per_page=per_page)
            if result:
                operations = [self.cd.batch_get_attributes(
                    obj_ref[object_selection]['Selector'],
                    node._facet,
                    ['name'])
                    for obj_ref in result]
                result = []
                for r in self.cd.batch_read(operations)['Responses']:
                    if r.get('SuccessfulResponse'):
                        result.append(
                            r.get('SuccessfulResponse')['GetObjectAttributes']['Attributes'][0]['Value']['StringValue'])
                    else:
                        logger.error({"message": "Batch Request Failed", "response": r})  # log error request failed
            return result, next_token
        else:
            return [
                type_link[object_selection]['Selector']
                for type_link in
                get_links(self.object_ref, filter_attribute_ranges, facet)
            ]

    def _add_links_batch(self, links: List[str], object_Type: str):
        """
        Attaches links to this object in CloudDirectory.
        """
        if not links:
            return []
        parent_path = self.cd.get_obj_type_path(object_Type)
        batch_attach_object = self.cd.batch_attach_object
        operations = []
        for link in links:
            parent_ref = f"{parent_path}{self.hash_name(link)}"
            operations.append(
                batch_attach_object(
                    parent_ref,
                    self.object_ref,
                    self._get_link_name(parent_ref, self.object_ref)
                )
            )
        return operations

    def _add_typed_links_batch(self, links: List[str], object_type, link_type: str, attributes: Dict, incoming=False):
        """
        Attaches links to this object in CloudDirectory.
        """
        if not links:
            return []
        link_path = self.cd.get_obj_type_path(object_type)
        batch_attach_typed_link = self.cd.batch_attach_typed_link
        operations = []
        for link in links:
            if incoming:
                source, target = f"{link_path}{self.hash_name(link)}", self.object_ref
            else:
                source, target = self.object_ref, f"{link_path}{self.hash_name(link)}"
            operations.append(batch_attach_typed_link(source, target, link_type, attributes))
        return operations

    def _remove_links_batch(self, links: List[str], link_type: str, incoming=False):
        """
        Removes links from this object in CloudDirectory.
        """
        if not links:
            return []
        link_path = self.cd.get_obj_type_path(link_type)
        batch_detach_object = self.cd.batch_detach_object
        operations = []
        for link in links:
            if incoming:
                source, target = f"{link_path}{self.hash_name(link)}", self.object_ref
            else:
                source, target = self.object_ref, f"{link_path}{self.hash_name(link)}"
            operations.append(
                batch_detach_object(
                    target,
                    self._get_link_name(target, source)
                )
            )
        return operations

    def _remove_typed_links_batch(self, links: List[str], object_type, link_type: str, attributes: Dict,
                                  incoming=False):
        """
        Removes links from this object in CloudDirectory.
        """
        if not links:
            return []
        link_path = self.cd.get_obj_type_path(object_type)
        batch_detach_typed_link = self.cd.batch_detach_typed_link
        make_typed_link_specifier = self.cd.make_typed_link_specifier
        operations = []
        for link in links:
            if incoming:
                source, target = f"{link_path}{self.hash_name(link)}", self.object_ref
            else:
                source, target = self.object_ref, f"{link_path}{self.hash_name(link)}"
            typed_link_specifier = make_typed_link_specifier(
                source,
                target,
                link_type,
                attributes
            )
            operations.append(batch_detach_typed_link(typed_link_specifier))
        return operations

    @property
    def name(self):
        if not self._name:
            self._get_attributes(self._attributes)
            self._path_name = self.hash_name(self._name)
        return self._name

    def _get_attributes(self, attributes: List[str]):
        """
        retrieve attributes for this from CloudDirectory and sets local private variables.
        """
        if not attributes:
            attributes = self._attributes
        try:
            resp = self.cd.get_object_attributes(self.object_ref, self._facet, attributes)
        except cd_client.exceptions.ResourceNotFoundException:
            raise FusilladeNotFoundException(detail="Resource does not exist.")
        for attr in resp['Attributes']:
            self.__setattr__('_' + attr['Key']['Name'], attr['Value'].popitem()[1])

    def get_attributes(self, attributes: List[str]) -> Dict[str, str]:
        try:
            resp = self.cd.get_object_attributes(self.object_ref, self._facet, attributes)
        except cd_client.exceptions.ResourceNotFoundException:
            raise FusilladeNotFoundException(detail="Resource does not exist.")
        return dict([(attr['Key']['Name'], attr['Value'].popitem()[1]) for attr in resp['Attributes']])

    @classmethod
    def list_all(cls, directory: CloudDirectory, next_token: str, per_page: int):
        resp, next_token = directory.list_object_children_paged(f'/{cls.object_type}/', next_token, per_page)
        operations = [directory.batch_get_attributes(f'${obj_ref}', cls._facet, ['name'])
                      for obj_ref in resp.values()]
        results = []
        for r in directory.batch_read(operations)['Responses']:
            if r.get('SuccessfulResponse'):
                results.append(
                    r.get('SuccessfulResponse')['GetObjectAttributes']['Attributes'][0]['Value']['StringValue'])
            else:
                logger.error({"message": "Batch Request Failed", "response": r})  # log error request failed
        return {f"{cls.object_type}s": results}, next_token

    def get_info(self) -> Dict[str, Any]:
        info = dict(**self.get_attributes(self._attributes))
        info[f'{self.object_type}_id'] = info.pop('name')
        return info


class PolicyMixin:
    """Adds policy support to a cloudNode"""
    allowed_policy_types = ['IAMPolicy']

    def lookup_policies(self) -> List[str]:
        policy_paths = self.cd.lookup_policy(self.object_ref)
        return self.cd.get_policies(policy_paths)

    def create_policy(self, statement: str, policy_type='IAMPolicy', **kwargs) -> str:
        """
        Create a policy object and attach it to the CloudNode
        :param statement: Json string that follow AWS IAM Policy Grammar.
          https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_grammar.html
        :return:
        """
        operations = list()
        object_attribute_list = self.cd.get_policy_attribute_list('IAMPolicy', statement, **kwargs)
        policy_link_name = self.get_policy_name(policy_type)
        parent_path = self.cd.get_obj_type_path('policy')
        operations.append(
            {
                'CreateObject': {
                    'SchemaFacet': [
                        {
                            'SchemaArn': self.cd.schema,
                            'FacetName': policy_type
                        },
                        {
                            'SchemaArn': self.cd.node_schema,
                            'FacetName': 'POLICY'
                        },
                    ],
                    'ObjectAttributeList': object_attribute_list,
                    'ParentReference': {
                        'Selector': parent_path
                    },
                    'LinkName': policy_link_name,
                }
            }
        )
        policy_ref = parent_path + policy_link_name

        operations.append(self.cd.batch_attach_policy(policy_ref, self.object_ref))
        self.cd.batch_write(operations)
        logger.info(dict(message="Policy created",
                         object=dict(
                             type=self.object_type,
                             path_name=self._path_name
                         ),
                         policy=dict(
                             link_name=policy_link_name,
                             policy_type="IAMPolicy")
                         ))
        return policy_ref

    def get_policy_name(self, policy_type):
        return self.hash_name(f"{self._path_name}{self.object_type}{policy_type}")

    def get_policy(self, policy_type: str = 'IAMPolicy'):
        """
        Policy statements follow AWS IAM Policy Grammer. See for grammar details
        https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_grammar.html
        """
        if policy_type in self.allowed_policy_types:  # check if this policy type is allowed
            if not self.attached_policies.get(policy_type):  # check if we already have the policy
                policy_ref = self.cd.get_obj_type_path('policy') + self.get_policy_name(policy_type)
                try:
                    resp = self.cd.get_object_attributes(
                        policy_ref,
                        'POLICY',
                        ['policy_document', 'policy_type'],
                        self.cd.node_schema
                    )
                    attrs = dict([(attr['Key']['Name'], attr['Value'].popitem()[1]) for attr in resp['Attributes']])
                    if attrs['policy_type'] != policy_type:
                        logger.warning({'message': "Retrieved policy_type does not match requested policy_type.",
                                        'expected': policy_type,
                                        'received': attrs['policy_type']
                                        })
                    self.attached_policies[policy_type] = attrs['policy_document'].decode("utf-8")
                except cd_client.exceptions.ResourceNotFoundException:
                    pass
            return self.attached_policies.get(policy_type, '')
        else:
            FusilladeHTTPException(
                title='Bad Request',
                detail=f"{self.object_type} cannot have policy type {policy_type}."
                f" Allowed types are: {self.allowed_policy_types}")

    def set_policy(self, statement: str, policy_type: str = 'IAMPolicy'):
        if policy_type in self.allowed_policy_types:
            try:
                # check if this object exists
                self.cd.get_object_information(self.object_ref)
            except cd_client.exceptions.ResourceNotFoundException:
                raise FusilladeNotFoundException(detail="Resource does not exist.")
            else:
                self._verify_statement(statement)
                self._set_policy(statement, policy_type)

    def _set_policy(self, statement: str, policy_type: str):
        params = [
            UpdateObjectParams('POLICY',
                               'policy_document',
                               ValueTypes.BinaryValue,
                               statement,
                               UpdateActions.CREATE_OR_UPDATE,
                               )
        ]
        try:
            try:
                self.cd.update_object_attribute(self.cd.get_obj_type_path('policy') + self.get_policy_name(policy_type),
                                                params,
                                                self.cd.node_schema)
            except cd_client.exceptions.ResourceNotFoundException:
                self.create_policy(statement, policy_type)
        except cd_client.exceptions.LimitExceededException as ex:
            raise FusilladeHTTPException(ex)
        else:
            logger.info(dict(message="Policy updated",
                             object=dict(
                                 type=self.object_type,
                                 path_name=self._path_name
                             ),
                             policy=dict(
                                 link_name=self.get_policy_name(policy_type),
                                 policy_type=policy_type)
                             ))

        self.attached_policies[policy_type] = None

    @retry(timeout=1, delay=0.1)
    def _set_policy_with_retry(self, statement, policy_type: str = 'IAMPolicy'):
        """
        Its possible for self._set_policy to fail with resource not found when the node is first created due to
        race conditions in creating new nodes in cloud directory. Retries give the cloud directory time to finish
        creating node before adding a new policy statement.
        :param policy:
        :return:
        """
        if policy_type in self.allowed_policy_types:
            self._set_policy(statement, policy_type)

    @staticmethod
    def _verify_statement(statement):
        """
        Verifies the policy statement is syntactically correct based on AWS's IAM Policy Grammar.
        A fake ActionNames and ResourceArns are used to facilitate the simulation of the policy.
        """

        try:
            iam.simulate_custom_policy(PolicyInputList=[statement],
                                       ActionNames=["fake:action"],
                                       ResourceArns=["arn:aws:iam::123456789012:user/Bob"])
        except iam.exceptions.InvalidInputException:
            raise FusilladeHTTPException(status=400, title="Bad Request", detail="Invalid policy format.")

    def get_policy_info(self):
        return {'policies': dict([(i, self.get_policy(i)) for i in self.allowed_policy_types if self.get_policy(i)])}


class CreateMixin(PolicyMixin):
    """Adds creation support to a cloudNode"""

    @classmethod
    def create(cls, cloud_directory: CloudDirectory, name: str, statement: Optional[str] = None,
               creator=None) -> Type['CloudNode']:
        if not statement:
            statement = get_json_file(cls._default_policy_path)
        cls._verify_statement(statement)
        _creator = creator if creator else "fusillade"
        try:
            cloud_directory.create_object(cls.hash_name(name), cls._facet, name=name, obj_type=cls.object_type,
                                          created_by=_creator)
        except cd_client.exceptions.LinkNameAlreadyInUseException:
            raise FusilladeHTTPException(
                status=409, title="Conflict", detail=f"The {cls.object_type} named {name} already exists.")
        new_node = cls(cloud_directory, name)
        if creator:
            User(cloud_directory, name=creator).add_ownership(new_node)
        logger.info(dict(message=f"{cls.object_type} created by {_creator}",
                         object=dict(type=new_node.object_type, path_name=new_node._path_name)))
        new_node._set_policy_with_retry(statement)
        return new_node


class RolesMixin:
    """Adds role support to a cloudNode"""

    @property
    def roles(self) -> List[str]:
        if not self._roles:
            self._roles = self._get_links(Role,
                                          'member_of',
                                          Role.object_type,
                                          'membership_link',
                                          incoming=False)
        return self._roles

    def get_roles(self, next_token: str = None, per_page: str = None):
        result, next_token = self._get_links(Role,
                                             'member_of',
                                             Role.object_type,
                                             'membership_link',
                                             paged=True,
                                             next_token=next_token,
                                             per_page=per_page)
        return {'roles': result}, next_token

    def add_roles(self, roles: List[str]):
        operations = []
        operations.extend(self._add_links_batch(roles, Role.object_type))
        operations.extend(self._add_typed_links_batch(roles,
                                                      Role.object_type,
                                                      'membership_link',
                                                      {'member_of': Role.object_type}))
        self.cd.batch_write(operations)
        self._roles = None  # update roles
        logger.info(dict(message="Roles added",
                         object=dict(type=self.object_type, path_name=self._path_name),
                         roles=roles))

    def remove_roles(self, roles: List[str]):
        operations = []
        operations.extend(self._remove_links_batch(roles, Role.object_type))
        operations.extend(self._remove_typed_links_batch(roles,
                                                         Role.object_type,
                                                         'membership_link',
                                                         {'member_of': Role.object_type}))
        self.cd.batch_write(operations)
        self._roles = None  # update roles
        logger.info(dict(message="Roles removed",
                         object=dict(type=self.object_type, path_name=self._path_name),
                         roles=roles))


class OwnershipMixin:
    ownable = ['group', 'role']

    def add_ownership(self, node: Type['CloudNode']):
        self.cd.attach_typed_link(
            self.object_ref,
            node.object_ref,
            'ownership_link',
            {'owner_of': node.object_type})

    def remove_ownership(self, node: Type['CloudNode']):
        typed_link_specifier = self.cd.make_typed_link_specifier(
            self.object_ref,
            node.object_ref,
            'ownership_link',
            {'owner_of': node.object_type}
        )
        self.cd.detach_typed_link(typed_link_specifier)

    def is_owner(self, node: Type['CloudNode']):
        tls = self.cd.make_typed_link_specifier(
            self.object_ref,
            node.object_ref,
            'ownership_link',
            {'owner_of': node.object_type})
        try:
            self.cd.get_link_attributes(tls, [])
        except cd_client.exceptions.ResourceNotFoundException:
            return False
        else:
            return True

    def list_owned(self, node: Type['CloudNode'], **kwargs):
        result, next_token = self._get_links(node=node,
                                             facet='ownership_link',
                                             attribute_value=node.object_type,
                                             attribute_name='owner_of',
                                             incoming=False,
                                             **kwargs)
        return {f"{node.object_type}s": result}, next_token

    def get_owned(self, object_type, **kwargs):
        if object_type in self.ownable:
            if object_type == 'group':
                return self.list_owned(Group, **kwargs)
            if object_type == 'role':
                return self.list_owned(Role, **kwargs)


class User(CloudNode, RolesMixin, PolicyMixin, OwnershipMixin):
    """
    Represents a user in CloudDirectory
    """
    _attributes = ['status'] + CloudNode._attributes
    default_roles = []  # TODO: make configurable
    default_groups = ['user_default']  # TODO: make configurable
    _facet = 'LeafFacet'
    object_type = 'user'

    def __init__(self, cloud_directory: CloudDirectory, name: str = None, object_ref: str = None):
        """

        :param cloud_directory:
        :param name:
        """
        super(User, self).__init__(cloud_directory,
                                   name=name,
                                   object_ref=object_ref)
        self._status = None
        self._groups: Optional[List[str]] = None
        self._roles: Optional[List[str]] = None

    def lookup_policies(self) -> List[str]:
        try:
            policy_paths = self.lookup_policies_batched()
        except cd_client.exceptions.ResourceNotFoundException:
            self.provision_user(self.cd, self.name)
            policy_paths = self.lookup_policies_batched()
        return self.cd.get_policies(policy_paths)

    def lookup_policies_batched(self):
        object_refs = self.groups + [self.object_ref]
        operations = [self.cd.batch_lookup_policy(object_ref) for object_ref in object_refs]
        all_results = []
        while True:
            results = [r['SuccessfulResponse']['LookupPolicy'] for r in self.cd.batch_read(operations)['Responses']]
            ops_index_modifier = 0
            for i in range(len(results)):
                all_results.extend(results[i]['PolicyToPathList'])  # get results
                if results[i].get('NextToken'):
                    operations[i - ops_index_modifier]['LookupPolicy']['NextToken'] = results[i]['NextToken']
                else:
                    operations.pop(i - ops_index_modifier)
                    ops_index_modifier += 1
            if not operations:
                break
        return all_results

    @property
    def status(self):
        if not self._status:
            self._get_attributes(['status'])
        return self._status

    def enable(self):
        """change the status of a user to enabled"""
        update_params = [
            UpdateObjectParams(self._facet,
                               'status',
                               ValueTypes.StringValue,
                               'Enabled',
                               UpdateActions.CREATE_OR_UPDATE)
        ]
        self.cd.update_object_attribute(self.object_ref, update_params)
        logger.info(dict(message="User Enabled", object=dict(type=self.object_type, path_name=self._path_name)))
        self._status = None

    def disable(self):
        """change the status of a user to disabled"""
        update_params = [
            UpdateObjectParams(self._facet,
                               'status',
                               ValueTypes.StringValue,
                               'Disabled',
                               UpdateActions.CREATE_OR_UPDATE)
        ]
        self.cd.update_object_attribute(self.object_ref, update_params)
        logger.info(dict(message="User Disabled", object=dict(type=self.object_type, path_name=self._path_name)))
        self._status = None

    @classmethod
    def provision_user(
            cls,
            cloud_directory: CloudDirectory,
            name: str,
            statement: Optional[str] = None,
            roles: List[str] = None,
            groups: List[str] = None,
            creator: str = None
    ) -> 'User':
        """
        Creates a user in cloud directory if the users does not already exists.

        :param cloud_directory:
        :param name:
        :param statement:
        :param roles:
        :param groups:
        :return:
        """
        user = cls(cloud_directory, name)
        _creator = creator if creator else "fusillade"
        try:
            user.cd.create_object(user._path_name,
                                  user._facet,
                                  name=user.name,
                                  status='Enabled',
                                  obj_type=cls.object_type,
                                  created_by=_creator
                                  )
        except cd_client.exceptions.LinkNameAlreadyInUseException:
            raise FusilladeHTTPException(
                status=409, title="Conflict", detail=f"The {cls.object_type} named {name} already exists.")
        else:
            logger.info(dict(message=f"{user.object_ref} created by {_creator}",
                             object=dict(type=user.object_type, path_name=user._path_name)))
        if roles:
            user.add_roles(roles + cls.default_roles)
        else:
            user.add_roles(cls.default_roles)

        if groups:
            user.add_groups(groups + cls.default_groups)
        else:
            user.add_groups(cls.default_groups)

        if statement:  # TODO make using user default configurable
            user._verify_statement(statement)
            user._set_policy_with_retry(statement)
        return user

    @property
    def groups(self) -> List[str]:
        if not self._groups:
            self._groups = self._get_links(Group,
                                           'member_of',
                                           Group.object_type,
                                           'membership_link')
        return self._groups

    def get_groups(self, next_token: str = None, per_page: int = None):
        result, next_token = self._get_links(Group,
                                             'member_of',
                                             Group.object_type,
                                             'membership_link',
                                             paged=True,
                                             next_token=next_token,
                                             per_page=per_page)
        return {'groups': result}, next_token

    def add_groups(self, groups: List[str]):
        operations = []

        operations.extend(self._add_typed_links_batch(groups,
                                                      Group.object_type,
                                                      'membership_link',
                                                      {'member_of': Group.object_type}))
        self.cd.batch_write(operations)
        self._groups = None  # update groups
        logger.info(dict(message="Groups joined",
                         object=dict(type=self.object_type, path_name=self._path_name),
                         groups=groups))

    def remove_groups(self, groups: List[str]):
        operations = []
        operations.extend(self._remove_typed_links_batch(groups,
                                                         Group.object_type,
                                                         'membership_link',
                                                         {'member_of': Group.object_type}))
        self.cd.batch_write(operations)
        self._groups = None  # update groups
        logger.info(dict(message="Groups left",
                         object=dict(type=self.object_type, path_name=self._path_name),
                         groups=groups))

    def get_info(self):
        info = super(User, self).get_info()
        info.update(super(User, self).get_policy_info())
        return info


class Group(CloudNode, RolesMixin, CreateMixin, OwnershipMixin):
    """
    Represents a group in CloudDirectory
    """
    _facet = 'LeafFacet'
    object_type = 'group'
    _default_policy_path = default_group_policy_path

    def __init__(self, cloud_directory: CloudDirectory, name: str = None, object_ref: str = None):
        """

        :param cloud_directory:
        :param name:
        """
        super(Group, self).__init__(cloud_directory, name=name, object_ref=object_ref)
        self._groups: Optional[List[str]] = None
        self._roles: Optional[List[str]] = None

    def get_users_iter(self) -> Tuple[Dict[str, Union[list, Any]], Any]:
        """
        Retrieves the object_refs for all user in this group.
        :return: (user name, user object reference)
        """
        return self._get_links(
            User,
            'member_of',
            self.object_type,
            'membership_link',
            incoming=True)

    def get_users_page(self, next_token=None, per_page=None) -> Tuple[Dict, str]:
        """
        Retrieves the object_refs for all user in this group.
        :return: (user name, user object reference)
        """
        results, next_token = self._get_links(
            User,
            'member_of',
            self.object_type,
            'membership_link',
            paged=True,
            per_page=per_page,
            incoming=True,
            next_token=next_token)
        return {'users': results}, next_token

    def add_users(self, users: List['User']) -> None:
        if users:
            operations = [
                self.cd.batch_attach_typed_link(
                    i.object_ref,
                    self.object_ref,
                    'membership_link',
                    {'member_of': 'group'}
                )
                for i in users]
            self.cd.batch_write(operations)
            logger.info(dict(message="Adding users to group",
                             object=dict(type=self.object_type, path_name=self._path_name),
                             users=[user._path_name for user in users]))

    def remove_users(self, users: List[str]) -> None:
        """
        Removes users from this group.

        :param users: a list of user names to remove from group
        :return:
        """
        for user in users:
            User(self.cd, user).remove_groups([self._path_name])
        logger.info(dict(message="Removing users from group",
                         object=dict(type=self.object_type, path_name=self._path_name),
                         users=[user for user in users]))

    def get_info(self):
        info = super(Group, self).get_info()
        info.update(self.get_policy_info())
        return info


class Role(CloudNode, CreateMixin):
    """
    Represents a role in CloudDirectory
    """
    _facet: str = 'NodeFacet'
    object_type: str = 'role'
    _default_policy_path: str = default_role_path

    def __init__(self, cloud_directory: CloudDirectory, name: str = None, object_ref: str = None):
        super(Role, self).__init__(cloud_directory, name=name, object_ref=object_ref)

    def get_info(self):
        info = super(Role, self).get_info()
        info.update(self.get_policy_info())
        return info

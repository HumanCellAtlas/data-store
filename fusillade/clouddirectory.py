"""
clouddrectory.py

This modules is used to simplify access to AWS Cloud Directory. For more information on AWS Cloud Directory see
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/clouddirectory.html

"""
import os
from dcplib.aws import clients as aws_clients
import functools
import json
import typing
from collections import namedtuple
from enum import Enum, auto
from urllib.parse import quote, unquote

from fusillade.errors import FusilladeException
from fusillade.config import Config

project_arn = "arn:aws:clouddirectory:us-east-1:861229788715:"  # TODO move to config.py
cd_client = aws_clients.clouddirectory

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


def cleanup_schema(sch_arn: str) -> None:
    cd_client.delete_schema(SchemaArn=sch_arn)


def publish_schema(name: str, version: str) -> str:
    """
    More info about schemas
    https://docs.aws.amazon.com/clouddirectory/latest/developerguide/schemas.html
    """
    # don't create if already created
    try:
        dev_schema_arn = cd_client.create_schema(Name=name)['SchemaArn']
    except cd_client.exceptions.SchemaAlreadyExistsException:
        dev_schema_arn = f"{project_arn}schema/development/{name}"

    # update the schema
    schema = get_json_file(directory_schema_path)
    cd_client.put_schema_from_json(SchemaArn=dev_schema_arn, Document=schema)
    try:
        pub_schema_arn = cd_client.publish_schema(DevelopmentSchemaArn=dev_schema_arn,
                                                  Version=version)['PublishedSchemaArn']
    except cd_client.exceptions.SchemaAlreadyPublishedException:
        pub_schema_arn = f"{project_arn}schema/published/{name}/{version}"
    return pub_schema_arn


def create_directory(name: str, schema: str) -> 'CloudDirectory':
    """
    Retrieve the fusillade cloud directory or do a one time setup of cloud directory to be used with fusillade.

    :param name:
    :param schema:
    :return:
    """
    try:
        response = cd_client.create_directory(
            Name=name,
            SchemaArn=schema
        )
        directory = CloudDirectory(response['DirectoryArn'])
    except cd_client.exceptions.DirectoryAlreadyExistsException:
        directory = CloudDirectory.from_name(name)
    else:
        # create structure
        for folder_name in ('Groups', 'Users', 'Roles', 'Policies'):
            directory.create_folder('/', folder_name)

        # create roles
        Role.create(directory, "default_user", statement=get_json_file(default_user_role_path))
        Role.create(directory, "admin", statement=get_json_file(default_admin_role_path))

        # create admins
        for admin in Config.get_admin_emails():
            user = User(directory, admin)
            user.add_roles(['admin'])
    return directory


def _paging_loop(fn, key, upack_response, **kwarg):
    while True:
        resp = fn(**kwarg)
        for i in resp[key]:
            yield upack_response(i)
        kwarg['NextToken'] = resp.get("NextToken")
        if not kwarg['NextToken']:
            break


def list_directories(state: str = 'ENABLED') -> typing.Iterator:
    def unpack_response(i):
        return i

    return _paging_loop(cd_client.list_directories, 'Directories', unpack_response, state=state)


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


class CloudDirectory:
    _page_limit = 30  # This is the max allowed by AWS

    def __init__(self, directory_arn: str):
        self._dir_arn = directory_arn
        self._schema = None

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

    def list_object_children(self, object_ref: str) -> typing.Iterator[typing.Tuple[str, str]]:
        """
        a wrapper around CloudDirectory.Client.list_object_children with paging
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
                            IncludeAllLinksToEachParent: bool = True) -> typing.Iterator:
        """
        a wrapper around CloudDirectory.Client.list_object_parents with paging
        """
        if IncludeAllLinksToEachParent:
            def unpack_response(i):
                return '$' + i['ObjectIdentifier'], i['LinkName']

            return _paging_loop(cd_client.list_object_parents,
                                'ParentLinks',
                                unpack_response,
                                DirectoryArn=self._dir_arn,
                                ObjectReference={'Selector': object_ref},
                                ConsistencyLevel='EVENTUAL',
                                IncludeAllLinksToEachParent=IncludeAllLinksToEachParent,
                                MaxResults=self._page_limit
                                )
        else:
            return _paging_loop(cd_client.list_object_parents,
                                'Parents',
                                self._make_ref,
                                DirectoryArn=self._dir_arn,
                                ObjectReference={'Selector': object_ref},
                                ConsistencyLevel='EVENTUAL',
                                IncludeAllLinksToEachParent=IncludeAllLinksToEachParent,
                                MaxResults=self._page_limit
                                )

    def list_object_policies(self, object_ref: str) -> typing.Iterator[str]:
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

    def list_policy_attachments(self, policy: str) -> typing.Iterator[str]:
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

    @staticmethod
    def _make_ref(i):
        return '$' + i

    def create_object(self, link_name: str, obj_type: str, **kwargs) -> str:
        """
        Create an object and store in cloud directory.
        """
        object_attribute_list = self._get_object_attribute_list(facet=obj_type, **kwargs)
        parent_path = self.get_obj_type_path(obj_type)
        cd_client.create_object(DirectoryArn=self._dir_arn,
                                SchemaFacets=[
                                    {
                                        'SchemaArn': self.schema,
                                        'FacetName': obj_type
                                    },
                                ],
                                ObjectAttributeList=object_attribute_list,
                                ParentReference=dict(Selector=parent_path),
                                LinkName=link_name)
        object_ref = parent_path + link_name
        return object_ref

    def get_object_attributes(self, obj_ref: str, facet: str, attributes: typing.List[str]) -> typing.Dict[str, str]:
        """
        a wrapper around CloudDirectory.Client.get_object_attributes
        """
        return cd_client.get_object_attributes(DirectoryArn=self._dir_arn,
                                               ObjectReference={'Selector': obj_ref},
                                               SchemaFacet={
                                                   'SchemaArn': self.schema,
                                                   'FacetName': facet
                                               },
                                               AttributeNames=attributes
                                               )

    def _get_object_attribute_list(self, facet="User", **kwargs) -> typing.List[typing.Dict[str, typing.Any]]:
        return [dict(Key=dict(SchemaArn=self.schema, FacetName=facet, Name=k), Value=dict(StringValue=v))
                for k, v in kwargs.items()]

    def get_policy_attribute_list(self,
                                  policy_type: str,
                                  statement: str,
                                  facet: str = "IAMPolicy",
                                  **kwargs) -> typing.List[typing.Dict[str, typing.Any]]:
        """
        policy_type and policy_document are required field for a policy object. However only policy_type is used by
        fusillade. Statement is used to store policy information. See the section on Policies for more
        info https://docs.aws.amazon.com/clouddirectory/latest/developerguide/key_concepts_directory.html
        """
        kwargs["Statement"] = statement
        obj = self._get_object_attribute_list(facet=facet, **kwargs)
        obj.append(dict(Key=dict(
            SchemaArn=self.schema,
            FacetName=facet,
            Name='policy_type'),
            Value=dict(StringValue=f"{policy_type}_{facet}")))
        obj.append(
            dict(Key=dict(SchemaArn=self.schema,
                          FacetName=facet,
                          Name="policy_document"),
                 Value=dict(BinaryValue='None'.encode())))
        return obj

    def update_object_attribute(self,
                                object_ref: str,
                                update_params: typing.List[UpdateObjectParams]) -> typing.Dict[str, typing.Any]:
        """
        a wrapper around CloudDirectory.Client.update_object_attributes

        :param object_ref: The reference that identifies the object.
        :param update_params: a list of attributes to modify.
        :return:
        """
        updates = [
            {
                'ObjectAttributeKey': {
                    'SchemaArn': self.schema,
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
        """ A folder is just a Group"""
        schema_facets = [dict(SchemaArn=self.schema, FacetName="Group")]
        object_attribute_list = self._get_object_attribute_list(facet="Group", name=name)
        try:
            cd_client.create_object(DirectoryArn=self._dir_arn,
                                    SchemaFacets=schema_facets,
                                    ObjectAttributeList=object_attribute_list,
                                    ParentReference=dict(Selector=path),
                                    LinkName=name)
        except cd_client.exceptions.LinkNameAlreadyInUseException:
            pass

    def clear(self) -> None:
        for _, policy_ref in self.list_object_children('/Policies/'):
            self.delete_policy(policy_ref)
        for _, obj_ref in self.list_object_children('/Users/'):
            self.delete_object(obj_ref)
        for _, obj_ref in self.list_object_children('/Groups/'):
            self.delete_object(obj_ref)
        for name, obj_ref in self.list_object_children('/Roles/'):
            if name not in ["admin", "default_user"]:
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
        self.batch_write([self.batch_detach_policy(policy_ref, obj_ref)
                          for policy_ref in self.list_object_policies(obj_ref)])
        self.batch_write([self.batch_detach_object(parent_ref, link_name)
                          for parent_ref, link_name in self.list_object_parents(obj_ref)])
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
                            object_attribute_list: typing.List[str]) -> typing.Dict[str, typing.Any]:
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

    def batch_get_attributes(self, obj_ref, facet, attributes: typing.List[str]) -> typing.Dict[str, typing.Any]:
        """
        A helper function to format a batch get_attributes operation
        """
        return {
            'GetObjectAttributes': {
                'ObjectReference': {
                    'Selector': obj_ref
                },
                'SchemaFacet': {
                    'SchemaArn': self.schema,
                    'FacetName': facet
                },
                'AttributeNames': attributes
            }
        }

    @staticmethod
    def batch_attach_object(parent: str, child: str, name: str) -> typing.Dict[str, typing.Any]:
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
    def batch_detach_object(parent: str, link_name: str) -> typing.Dict[str, typing.Any]:
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
    def batch_attach_policy(policy: str, object_ref: str) -> typing.Dict[str, typing.Any]:
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

    def batch_write(self, operations: list) -> typing.Dict[str, typing.Any]:
        """
        A wrapper around CloudDirectory.Client.batch_write
        """
        return cd_client.batch_write(DirectoryArn=self._dir_arn, Operations=operations)

    def batch_read(self, operations: typing.List[typing.Dict[str, typing.Any]]) -> typing.Dict[str, typing.Any]:
        """
        A wrapper around CloudDirectory.Client.batch_read
        """
        return cd_client.batch_read(DirectoryArn=self._dir_arn, Operations=operations)

    @staticmethod
    def get_obj_type_path(obj_type: str) -> str:
        obj_type = obj_type.lower()
        paths = dict(group='/Groups/',
                     index='/Indices/',
                     user='/Users/',
                     policy='/Policies/',
                     role='/Roles/')
        return paths[obj_type]

    def lookup_policy(self, object_id: str) -> typing.List[str]:
        max_results = 3  # Max recommended by AWS Support

        # retrieve all of the policies attached to an object and its parents.
        response = cd_client.lookup_policy(
            DirectoryArn=self._dir_arn,
            ObjectReference={'Selector': object_id},
            MaxResults=max_results
        )
        policies_paths: list = response['PolicyToPathList']
        while response.get('NextToken'):
            response = cd_client.lookup_policy(
                DirectoryArn=self._dir_arn,
                ObjectReference={'Selector': object_id},
                NextToken=response['NextToken'],
                MaxResults=max_results
            )
            policies_paths.extend(response['PolicyToPathList'])

        # Parse the policyIds from the policies path. Only keep the unique ids
        policy_ids = set(
            [
                (o['PolicyId'], o['PolicyType'])
                for p in policies_paths
                for o in p['Policies']
                if o.get('PolicyId')
            ]
        )

        # retrieve the policies in a single request
        operations = [
            {
                'GetObjectAttributes': {
                    'ObjectReference': {'Selector': f'${policy_id[0]}'},
                    'SchemaFacet': {
                        'SchemaArn': self.schema,
                        'FacetName': 'IAMPolicy'
                    },
                    'AttributeNames': ['Statement']
                }
            }
            for policy_id in policy_ids
        ]

        # parse the policies from the responses
        policies = [
            response['SuccessfulResponse']['GetObjectAttributes']['Attributes'][0]['Value']['StringValue']
            for response in cd_client.batch_read(DirectoryArn=self._dir_arn, Operations=operations)['Responses']
        ]
        return policies

    def get_object_information(self, obj_ref: str) -> typing.Dict[str, typing.Any]:
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


class CloudNode:
    """
    Contains shared code across the different types of nodes stored in Fusillade CloudDirectory
    """
    _attributes = ["name"]  # the different attributes of a node stored
    _link_formats = {"group": "G->{parent}->{child}", "role": "R->{parent}->{child}"}

    # The format string for the links that connect different nodes in cloud directory

    def __init__(self, cloud_directory: CloudDirectory, name: str, object_type: str):
        """

        :param cloud_directory:
        :param name:
        """
        self._name: str = name
        self._object_type: str = object_type
        self.cd: CloudDirectory = cloud_directory
        self._path_name: str = quote(name)
        self.object_reference: str = cloud_directory.get_obj_type_path(object_type) + self._path_name
        self._policy: typing.Optional[str] = None
        self._statement: typing.Optional[str] = None

    def _get_links(self):
        """
        Retrieves the links attached to this object from CloudDirectory and separates them into groups and roles
        based on the link name
        """
        self._roles = []
        self._groups = []
        for _, link_name in self.cd.list_object_parents(self.object_reference):
            if link_name.startswith('G'):
                self._groups.append(unquote(link_name.split('->')[1]))
            elif link_name.startswith('R'):
                self._roles.append(unquote(link_name.split('->')[1]))

    def _add_links(self, links: typing.List[str], link_type: str):
        """
        Attaches links to this object in CloudDirectory.
        """
        if not links:
            return
        parent_path = self.cd.get_obj_type_path(link_type)
        operations = [self.cd.batch_attach_object(parent_path + link,
                                                  self.object_reference,
                                                  self._link_formats[link_type].format(parent=quote(link),
                                                                                       child=self._path_name))
                      for link in links]
        self.cd.batch_write(operations)

    def _remove_links(self, links: typing.List[str], link_type: str):
        """
        Removes links from this object in CloudDirectory.
        """
        if not links:
            return
        parent_path = self.cd.get_obj_type_path(link_type)
        operations = [self.cd.batch_detach_object(parent_path + link,
                                                  self._link_formats[link_type].format(parent=quote(link),
                                                                                       child=self._path_name))
                      for link in links]
        self.cd.batch_write(operations)

    def lookup_policies(self) -> typing.List[str]:
        return self.cd.lookup_policy(self.object_reference)

    @property
    def name(self):
        return self._name

    @property
    def policy(self):
        if not self._policy:
            policies = [i for i in self.cd.list_object_policies(self.object_reference)]
            if not policies:
                return None
            elif len(policies) > 1:
                raise ValueError("Node has multiple policies attached")
            else:
                self._policy = policies[0]
        return self._policy

    def create_policy(self, statement: str, ) -> str:
        """
        Create a policy object and attach it to the CloudNode
        :param statement: Json string that follow AWS IAM Policy Grammar.
          https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_grammar.html
        :return:
        """
        operations = list()
        object_attribute_list = self.cd.get_policy_attribute_list(self._object_type, statement)
        policy_link_name = f"{self._path_name}_{self._object_type}_IAMPolicy"
        parent_path = self.cd.get_obj_type_path('policy')
        operations.append(self.cd.batch_create_object(parent_path,
                                                      policy_link_name,
                                                      'IAMPolicy',
                                                      object_attribute_list))
        policy_ref = parent_path + policy_link_name

        operations.append(self.cd.batch_attach_policy(policy_ref, self.object_reference))
        self.cd.batch_write(operations)
        return policy_ref

    @property
    def statement(self):
        """
        Policy statements follow AWS IAM Policy Grammer. See for grammar details
        https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_grammar.html
        """
        if not self._statement and self.policy:
            self._statement = self.cd.get_object_attributes(self.policy,
                                                            'IAMPolicy',
                                                            ['Statement'])['Attributes'][0]['Value'].popitem()[1]

        return self._statement

    @statement.setter
    def statement(self, statement: str):
        self._verify_statement(statement)
        self._set_statement(statement)

    def _set_statement(self, statement: str):
        if not self.policy:
            self.create_policy(statement)
        else:
            params = [
                UpdateObjectParams('IAMPolicy',
                                   'Statement',
                                   ValueTypes.StringValue,
                                   statement,
                                   UpdateActions.CREATE_OR_UPDATE)
            ]
            self.cd.update_object_attribute(self.policy, params)
        self._statement = None

    def _set_attributes(self, attributes: typing.List[str]):
        """
        retrieve attributes for this from CloudDirectory and sets local private variables.
        """
        resp = self.cd.get_object_attributes(self.object_reference, self._object_type, attributes)
        for attr in resp['Attributes']:
            self.__setattr__('_' + attr['Key']['Name'], attr['Value'].popitem()[1])

    def get_attributes(self, attributes: typing.List[str]):
        attrs = dict()
        if not attributes:
            return attrs
        resp = self.cd.get_object_attributes(self.object_reference, self._object_type, attributes)
        for attr in resp['Attributes']:
            attrs[attr['Key']['Name']] = attr['Value'].popitem()[1]  # noqa
        return attrs

    @staticmethod
    def _verify_statement(statement):
        """
        Verifies the policy statement is syntactically correct based on AWS's IAM Policy Grammar.
        A fake ActionNames and ResourceArns are used to facilitate the simulation of the policy.
        """
        iam = aws_clients.iam
        try:
            iam.simulate_custom_policy(PolicyInputList=[statement],
                                       ActionNames=["fake:action"],
                                       ResourceArns=["arn:aws:iam::123456789012:user/Bob"])
        except iam.exceptions.InvalidInputException as ex:
            raise FusilladeException from ex


class User(CloudNode):
    """
    Represents a user in CloudDirectory
    """
    _attributes = ['status'] + CloudNode._attributes

    def __init__(self, cloud_directory: CloudDirectory, name: str, local: bool = False):
        """

        :param cloud_directory:
        :param name:
        :param local: Set to True if you want to retrieve information from the external directory on initialization.
        """
        super(User, self).__init__(cloud_directory, name, 'User')
        self._status = None
        self._groups: typing.Optional[typing.List[str]] = None
        self._roles: typing.Optional[typing.List[str]] = None  # TODO make a property
        if not local:
            try:
                self._set_attributes(self._attributes)
            except cd_client.exceptions.ResourceNotFoundException:
                self.provision_user()
                self.add_roles(['default_user'])
                self._set_attributes(self._attributes)

    @property
    def status(self):
        if not self._status:
            self._set_attributes(['status'])
        return self._status

    def enable(self):
        """change the status of a user to enabled"""
        update_params = [
            UpdateObjectParams('User',
                               'status',
                               ValueTypes.StringValue,
                               'Enabled',
                               UpdateActions.CREATE_OR_UPDATE)
        ]
        self.cd.update_object_attribute(self.object_reference, update_params)

        self._status = None

    def disable(self):
        """change the status of a user to disabled"""
        update_params = [
            UpdateObjectParams('User',
                               'status',
                               ValueTypes.StringValue,
                               'Disabled',
                               UpdateActions.CREATE_OR_UPDATE)
        ]
        self.cd.update_object_attribute(self.object_reference, update_params)
        self._status = None

    def provision_user(self, statement: typing.Optional[str] = None) -> None:
        self.cd.create_object(self._path_name,
                              'User',
                              name=self.name,
                              status='Enabled')
        if statement:  # TODO make using user default configurable
            self.statement = statement

    @property
    def groups(self):
        if not self._groups:
            self._get_links()
        return self._groups

    def add_groups(self, groups: typing.List[str]):
        self._add_links(groups, 'group')
        self._groups = None  # update groups

    def remove_groups(self, groups: typing.List[str]):
        self._remove_links(groups, 'group')
        self._groups = None  # update groups

    @property
    def roles(self):
        if not self._roles:
            self._get_links()
        return self._roles

    def add_roles(self, roles: typing.List[str]):
        self._add_links(roles, 'role')
        self._roles = None  # update roles

    def remove_roles(self, roles: typing.List[str]):
        self._remove_links(roles, 'role')
        self._roles = None  # update roles


class Group(CloudNode):
    """
    Represents a group in CloudDirectory
    """

    def __init__(self, cloud_directory: CloudDirectory, name: str, local: bool = False):
        """

        :param cloud_directory:
        :param name:
        :param local: Set to True if you want to retrieve information from the external directory on initialization.
        """
        super(Group, self).__init__(cloud_directory, name, 'Group')
        self._groups = None
        self._roles = None
        if not local:
            self._set_attributes(self._attributes)

    @classmethod
    def create(cls,
               cloud_directory: CloudDirectory,
               name: str,
               statement: typing.Optional[str] = None) -> 'Group':
        if not statement:
            statement = get_json_file(default_group_policy_path)
        cls._verify_statement(statement)
        cloud_directory.create_object(quote(name), 'Group', name=name)
        new_node = cls(cloud_directory, name)
        new_node._set_statement(statement)
        return new_node

    def get_users(self) -> typing.Iterator[typing.Tuple[str, str]]:
        """
        Retrieves the object_references for all user in this group.
        :return: (user name, user object reference)
        """
        for link, ref in self.cd.list_object_children(self.object_reference):
            yield unquote(link.split('->')[1]), ref

    @property
    def roles(self):
        if not self._roles:
            self._get_links()
        return self._roles

    def add_roles(self, roles: typing.List[str]):
        self._add_links(roles, 'role')
        self._roles = None  # update roles

    def remove_roles(self, roles: typing.List[str]):
        self._remove_links(roles, 'role')
        self._roles = None  # update roles

    def add_users(self, users: typing.List[User]) -> None:
        if users:
            operations = [
                self.cd.batch_attach_object(self.object_reference,
                                            i.object_reference,
                                            self._link_formats['group'].format(parent=self._path_name,
                                                                               child=i._path_name))
                for i in users]
            self.cd.batch_write(operations)

    def remove_users(self, users: typing.List[str]) -> None:
        """
        Removes users from this group.

        :param users: a list of user names to remove from group
        :return:
        """
        for user in users:
            User(self.cd, user, local=True).remove_groups([self._path_name])


class Role(CloudNode):
    """
    Represents a role in CloudDirectory
    """

    def __init__(self, cloud_directory: CloudDirectory, name: str):
        super(Role, self).__init__(cloud_directory, name, 'Role')

    @classmethod
    def create(cls,
               cloud_directory: CloudDirectory,
               name: str,
               statement: typing.Optional[str] = None) -> 'Role':
        if not statement:
            statement = get_json_file(default_role_path)
        cls._verify_statement(statement)
        cloud_directory.create_object(quote(name), 'Role', name=name)
        new_node = cls(cloud_directory, name)
        new_node._set_statement(statement)
        return new_node

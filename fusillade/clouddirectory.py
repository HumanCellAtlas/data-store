import functools
import json
import typing
import boto3
from fusillade.errors import FusilladeException
from collections import namedtuple
from enum import Enum, auto
from urllib.parse import quote, unquote
import os

ad = boto3.client("clouddirectory")
arn = "arn:aws:clouddirectory:us-east-1:861229788715:"


def get_published_schema_from_directory(dir_arn: str) -> str:
    schema = ad.list_applied_schema_arns(DirectoryArn=dir_arn)['SchemaArns'][0]
    schema = schema.split('/')[-2:]
    schema = '/'.join(schema)
    return f"{arn}schema/published/{schema}"


def cleanup_directory(dir_arn: str):
    ad.disable_directory(DirectoryArn=dir_arn)
    ad.delete_directory(DirectoryArn=dir_arn)


def cleanup_schema(sch_arn: str):
    ad.delete_schema(SchemaArn=sch_arn)


def publish_schema(name: str, version: str):
    # don't create if already created
    try:
        dev_schema_arn = ad.create_schema(Name=name)['SchemaArn']
    except ad.exceptions.SchemaAlreadyExistsException:
        dev_schema_arn = f"{arn}schema/development/{name}"

    # update the schema
    pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
    with open(pkg_root + '/fusillade/directory_schema.json') as schema_json:
        schema = json.dumps(json.load(schema_json))
    ad.put_schema_from_json(SchemaArn=dev_schema_arn, Document=schema)
    try:
        pub_schema_arn = ad.publish_schema(DevelopmentSchemaArn=dev_schema_arn,
                                           Version=version)['PublishedSchemaArn']
    except ad.exceptions.SchemaAlreadyPublishedException:
        pub_schema_arn = f"{arn}schema/published/{name}/{version}"
    return pub_schema_arn


def create_directory(name: str, schema: str):
    try:
        response = ad.create_directory(
            Name=name,
            SchemaArn=schema
        )
        directory = CloudDirectory(response['DirectoryArn'])

        # create structure
        directory.create_folder('/', 'Groups')
        directory.create_folder('/', 'Users')
        directory.create_folder('/', 'Roles')
        directory.create_folder('/', 'Policies')

        # create roles
        with open("./policies/default_user_role.json", 'r') as fp:
            Role.create(directory, "default_user", statement=fp.read())
        with open("./policies/default_admin_role.json", 'r') as fp:
            Role.create(directory, "admin", statement=fp.read())

        # create admins
        for admin in os.environ['FUS_ADMIN_EMAILS'].split(','):
            user = User(directory, admin)
            user.add_roles(['admin'])

    except ad.exceptions.DirectoryAlreadyExistsException:
        directory = CloudDirectory.from_name(name)
    return directory


def list_directories(state: str = 'ENABLED') -> typing.Iterator:
    resp = ad.list_directories(state=state)
    while True:
        for i in resp['Directories']:
            yield i
        next_token = resp.get('NextToken')
        if next_token:
            ad.list_directories(state=state, NextToken=next_token)
        else:
            break


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
        self._schema = ad.list_applied_schema_arns(DirectoryArn=directory_arn)['SchemaArns'][0]

    @classmethod
    @functools.lru_cache()
    def from_name(cls, dir_name: str):
        # get directory arn by name
        for i in list_directories():
            if i['Name'] == dir_name:
                dir_arn = i['DirectoryArn']
                return cls(dir_arn)
        raise FusilladeException(f"{dir_name} does not exist")

    def list_object_children(self, object_ref: str) -> typing.Iterator[str]:
        resp = ad.list_object_children(DirectoryArn=self._dir_arn,
                                       ObjectReference={'Selector': object_ref},
                                       ConsistencyLevel='EVENTUAL',
                                       MaxResults=self._page_limit)
        while True:
            for child in resp['Children'].values():
                yield '$' + child
            next_token = resp.get('NextToken')
            if next_token:
                resp = ad.list_object_children(DirectoryArn=self._dir_arn,
                                               ObjectReference={'Selector': object_ref},
                                               NextToken=next_token,
                                               MaxResults=self._page_limit)
            else:
                break

    def list_object_parents(self,
                            object_ref: str,
                            IncludeAllLinksToEachParent: bool = True) -> typing.Iterator:
        resp = ad.list_object_parents(DirectoryArn=self._dir_arn,
                                      ObjectReference={'Selector': object_ref},
                                      ConsistencyLevel='EVENTUAL',
                                      IncludeAllLinksToEachParent=IncludeAllLinksToEachParent,
                                      MaxResults=self._page_limit
                                      )
        if IncludeAllLinksToEachParent:
            while True:
                for parent in resp['ParentLinks']:
                    yield '$' + parent['ObjectIdentifier'], parent['LinkName']
                next_token = resp.get('NextToken')
                if next_token:
                    resp = ad.list_object_parents(DirectoryArn=self._dir_arn,
                                                  ObjectReference={'Selector': object_ref},
                                                  NextToken=next_token,
                                                  MaxResults=self._page_limit)
                else:
                    break
        else:
            while True:
                for parent in resp['Parents']:
                    yield '$' + parent
                next_token = resp.get('NextToken')
                if next_token:
                    resp = ad.list_object_parents(DirectoryArn=self._dir_arn,
                                                  ObjectReference={'Selector': object_ref},
                                                  NextToken=next_token,
                                                  MaxResults=self._page_limit)
                else:
                    break

    def list_object_policies(self, object_ref: str) -> typing.Iterator[str]:
        resp = ad.list_object_policies(DirectoryArn=self._dir_arn,
                                       ObjectReference={'Selector': object_ref},
                                       MaxResults=self._page_limit)
        while True:
            for policy in resp['AttachedPolicyIds']:
                yield '$' + policy
            next_token = resp.get('NextToken')
            if next_token:
                resp = ad.list_object_policies(DirectoryArn=self._dir_arn,
                                               ObjectReference={'Selector': object_ref},
                                               NextToken=next_token,
                                               MaxResults=self._page_limit)
            else:
                break

    def list_policy_attachments(self, policy: str) -> typing.Iterator[str]:
        resp = ad.list_policy_attachments(DirectoryArn=self._dir_arn,
                                          PolicyReference={'Selector': policy})
        while True:
            for object_id in resp['ObjectIdentifiers']:
                yield '$' + object_id
            next_token = resp.get('NextToken')
            if next_token:
                resp = ad.list_policy_attachments(DirectoryArn=self._dir_arn,
                                                  PolicyReference=policy,
                                                  NextToken={'Selector': policy},
                                                  MaxResults=self._page_limit)
            else:
                break

    def create_object(self, link_name: str, statement: str, obj_type: str, **kwargs) -> str:
        operations = list()
        object_attribute_list = self._get_object_attribute_list(facet=obj_type, **kwargs)
        parent_path = self.get_obj_type_path(obj_type)
        operations.append(self.batch_create_object(parent_path, link_name, obj_type, object_attribute_list))
        object_ref = parent_path + link_name

        object_attribute_list = self.get_policy_attribute_list(f'{obj_type}Policy', statement, Statement=statement)
        policy_link_name = f"{link_name}_policy"
        parent_path = self.get_obj_type_path('policy')
        operations.append(self.batch_create_object(parent_path,
                                                   policy_link_name,
                                                   'IAMPolicy',
                                                   object_attribute_list))
        policy_ref = parent_path + policy_link_name

        operations.append(self.batch_attach_policy(policy_ref, object_ref))
        self.batch_write(operations)  # TODO check that everything passed

        return object_ref, policy_ref

    def get_object_attrtibutes(self, obj_ref: str, facet: str, attributes: typing.List[str]) -> typing.Dict[str, str]:
        return ad.get_object_attributes(DirectoryArn=self._dir_arn,
                                        ObjectReference={'Selector': obj_ref},
                                        SchemaFacet={
                                            'SchemaArn': self._schema,
                                            'FacetName': facet
                                        },
                                        AttributeNames=attributes
                                        )

    def _get_object_attribute_list(self, facet="User", **kwargs):
        return [dict(Key=dict(SchemaArn=self._schema, FacetName=facet, Name=k), Value=dict(StringValue=v))
                for k, v in kwargs.items()]

    def get_policy_attribute_list(self, policy_type: str, policy_document: str, facet: str = "IAMPolicy", **kwargs):
        obj = self._get_object_attribute_list(facet=facet, **kwargs)
        obj.append(dict(Key=dict(
            SchemaArn=self._schema,
            FacetName=facet,
            Name='policy_type'),
            Value=dict(StringValue=policy_type)))
        obj.append(
            dict(Key=dict(SchemaArn=self._schema,
                          FacetName="IAMPolicy",
                          Name="policy_document"),
                 Value=dict(BinaryValue=json.dumps(policy_document).encode())))
        return obj

    def update_object_attribute(self, object_ref: str, update_params: typing.List[UpdateObjectParams]):
        """

        :param object_ref: The reference that identifies the object.
        :param update_params:
        :return:
        """
        updates = []
        for i in update_params:
            updates.append(
                {
                    'ObjectAttributeKey': {
                        'SchemaArn': self._schema,
                        'FacetName': i.facet,
                        'Name': i.attribute
                    },
                    'ObjectAttributeAction': {
                        'ObjectAttributeActionType': i.action.name,
                        'ObjectAttributeUpdateValue': {
                            i.value_type.name: i.value
                        }
                    }
                }
            )
        return ad.update_object_attributes(
            DirectoryArn=self._dir_arn,
            ObjectReference={
                'Selector': object_ref
            },
            AttributeUpdates=updates
        )

    def create_folder(self, path, name):
        # A folder is just a Group
        schema_facets = [dict(SchemaArn=self._schema, FacetName="Group")]
        object_attribute_list = self._get_object_attribute_list(facet="Group", name=name)
        try:
            ad.create_object(DirectoryArn=self._dir_arn,
                             SchemaFacets=schema_facets,
                             ObjectAttributeList=object_attribute_list,
                             ParentReference=dict(Selector=path),
                             LinkName=name)
        except ad.exceptions.LinkNameAlreadyInUseException:
            pass

    def clear(self):
        for policy_ref in self.list_object_children('/Policies/'):
            self.delete_policy(policy_ref)
        for obj_ref in self.list_object_children('/Users/'):
            self.delete_object(obj_ref)
        for obj_ref in self.list_object_children('/Groups/'):
            self.delete_object(obj_ref)

    def delete_policy(self, policy_ref):
        self.batch_write([self.batch_detach_policy(policy_ref, obj_ref)
                          for obj_ref in self.list_policy_attachments(policy_ref)])
        self.batch_write([self.batch_detach_object(parent_ref, link_name)
                          for parent_ref, link_name in self.list_object_parents(policy_ref)])
        ad.delete_object(DirectoryArn=self._dir_arn, ObjectReference={'Selector': policy_ref})

    def delete_object(self, obj_ref):
        self.batch_write([self.batch_detach_policy(policy_ref, obj_ref)
                          for policy_ref in self.list_object_policies(obj_ref)])
        self.batch_write([self.batch_detach_object(parent_ref, link_name)
                          for parent_ref, link_name in self.list_object_parents(obj_ref)])
        ad.delete_object(DirectoryArn=self._dir_arn, ObjectReference={'Selector': obj_ref})

    @staticmethod
    def batch_detach_policy(policy_ref, object_ref):
        return {
            'DetachPolicy': {
                'PolicyReference': {'Selector': policy_ref},
                'ObjectReference': {'Selector': object_ref}
            }
        }

    def batch_create_object(self, parent, name, facet_name, object_attribute_list):
        """
        batch write operation
        :param parent:
        :param name:
        :param facet_name:
        :param object_attribute_list:
        :return:
        """
        return {'CreateObject': {
            'SchemaFacet': [
                {
                    'SchemaArn': self._schema,
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

    def batch_get_attributes(self, obj_ref, facet, attributes: typing.List[str]):
        return {
            'GetObjectAttributes': {
                'ObjectReference': {
                    'Selector': obj_ref
                },
                'SchemaFacet': {
                    'SchemaArn': self._schema,
                    'FacetName': facet
                },
                'AttributeNames': attributes
            }
        }

    @staticmethod
    def batch_attach_object(parent, child, name):
        """
        batch write operation
        :param parent:
        :param child:
        :param name:
        :return:
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
    def batch_detach_object(parent, link_name):
        return {'DetachObject': {
            'ParentReference': {
                'Selector': parent
            },
            'LinkName': link_name,
        }}

    @staticmethod
    def batch_attach_policy(policy, object_ref):
        """
        batch write operation
        :param policy:
        :param object_ref:
        :return:
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

    def batch_write(self, operations: list) -> dict:
        return ad.batch_write(DirectoryArn=self._dir_arn, Operations=operations)

    def batch_read(self, operations: list) -> dict:
        return ad.batch_read(DirectoryArn=self._dir_arn, Operations=operations)

    @staticmethod
    def get_obj_type_path(obj_type):
        obj_type = obj_type.lower()
        paths = dict(group='/Groups/',
                     index='/Indices/',
                     user='/Users/',
                     policy='/Policies/',
                     role='/Roles/')
        return paths[obj_type]

    def lookup_policy(self, object_id):
        max_results = 3

        # retrieve all of the policies attached to an object and its parents.
        response = ad.lookup_policy(
            DirectoryArn=self._dir_arn,
            ObjectReference={'Selector': object_id},
            MaxResults=max_results
        )
        policies_paths = response['PolicyToPathList']
        while response.get('NextToken'):
            response = ad.lookup_policy(
                DirectoryArn=self._dir_arn,
                ObjectReference={'Selector': object_id},
                NextToken=response['NextToken'],
                MaxResults=max_results
            )
            policies_paths += response['PolicyToPathList']

        # Parse the policyIds from the policies path. Only keep the unique ids
        policy_ids = set()
        for p in policies_paths:
            for o in p['Policies']:
                if o.get('PolicyId'):
                    policy_ids.add((o['PolicyId'], o['PolicyType']))

        # retrieve the policies in a single request
        operations = [{'GetObjectAttributes': {
            'ObjectReference': {'Selector': f'${policy_id[0]}'},
            'SchemaFacet': {'SchemaArn': self._schema, 'FacetName': 'IAMPolicy'},
            'AttributeNames': ['Statement']}} for policy_id in policy_ids]
        responses = ad.batch_read(
            DirectoryArn=self._dir_arn,
            Operations=operations
        )['Responses']

        # parse the policies from the responses
        policies = []
        for response in responses:
            policies.append(response['SuccessfulResponse']['GetObjectAttributes']['Attributes'][0]['Value'])
        return policies

    def get_object_information(self, obj_ref: str):
        return ad.get_object_information(
            DirectoryArn=self._dir_arn,
            ObjectReference={
                'Selector': obj_ref
            },
            ConsistencyLevel='EVENTUAL'
        )


class CloudNode:
    _attributes = ["name"]
    _link_formats = {"group": "G->{parent}->{child}", "role": "R->{parent}->{child}"}

    def __init__(self, cloud_directory: CloudDirectory, name: str, object_type):
        """

        :param cloud_directory:
        :param name:
        :param local: use if you don't want to retrieve information from the directory when initializing
        """
        self._name: str = name
        self._object_type: str = object_type
        self.cd: CloudDirectory = cloud_directory
        self._path_name: str = quote(name)
        self.object_reference: str = cloud_directory.get_obj_type_path(object_type) + self._path_name
        self._policy: typing.Optional[str] = None
        self._statement: typing.Optional[str] = None

    def _get_links(self):
        self._roles = []
        self._groups = []
        for _, link_name in self.cd.list_object_parents(self.object_reference):
            if link_name.startswith('G'):
                self._groups.append(unquote(link_name.split('->')[1]))
            elif link_name.startswith('R'):
                self._roles.append(unquote(link_name.split('->')[1]))

    def _add_links(self, links: typing.List[str], link_type):
        if not len(links):
            return
        parent_path = self.cd.get_obj_type_path(link_type)
        operations = [self.cd.batch_attach_object(parent_path + link,
                                                  self.object_reference,
                                                  self._link_formats[link_type].format(parent=quote(link),
                                                                                       child=self._path_name))
                      for link in links]
        self.cd.batch_write(operations)

    def _remove_links(self, links: typing.List[str], link_type):
        if not len(links):
            return
        parent_path = self.cd.get_obj_type_path(link_type)
        operations = [self.cd.batch_detach_object(parent_path + link,
                                                  self._link_formats[link_type].format(parent=quote(link),
                                                                                       child=self._path_name))
                      for link in links]
        self.cd.batch_write(operations)

    def lookup_policies(self) -> typing.List[str]:
        return [policy['StringValue'] for policy in self.cd.lookup_policy(self.object_reference)]

    @property
    def name(self):
        return self._name

    @property
    def policy(self):
        if not self._policy:
            self._policy = [i for i in self.cd.list_object_policies(self.object_reference)][0]
        return self._policy

    @property
    def statement(self):
        if not self._statement:
            self._statement = self.cd.get_object_attrtibutes(self.policy,
                                                             'IAMPolicy',
                                                             ['Statement'])['Attributes'][0]['Value'].popitem()[1]
        return self._statement

    @statement.setter
    def statement(self, statement):
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
        resp = self.cd.get_object_attrtibutes(self.object_reference, self._object_type, attributes)
        for attr in resp['Attributes']:
            self.__setattr__('_' + attr['Key']['Name'], attr['Value'].popitem()[1])

    def get_attributes(self, attributes: typing.List[str]):
        attrs = dict()
        if not attributes:
            return attrs
        resp = self.cd.get_object_attrtibutes(self.object_reference, self._object_type, attributes)
        for attr in resp['Attributes']:
            attrs[attr['Key']['Name']] = attr['Value'].popitem()[1]   # noqa
        return attrs


class User(CloudNode):
    _attributes = ['status'] + CloudNode._attributes

    def __init__(self, cloud_directory: CloudDirectory, name: str, local: bool = False):
        """

        :param cloud_directory:
        :param name:
        :param style: email|id
        :param local: use if you don't want to retrieve information from the directory when initializing
        """
        super(User, self).__init__(cloud_directory, name, 'User')
        self._status = None
        self._groups: typing.Optional[typing.List[str]] = None
        self._roles: typing.Optional[typing.List[str]] = None  # TODO make a property
        self._policy = None
        self._statement = None
        if not local:
            try:
                self._set_attributes(self._attributes)
            except ad.exceptions.ResourceNotFoundException:
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

    def provision_user(self, statement: str = None):
        if not statement:
            with open("./policies/default_user_policy.json", 'r') as fp:
                statement = json.load(fp)
        return self.cd.create_object(self._path_name,
                                     json.dumps(statement),
                                     'User',
                                     name=self.name,
                                     status='Enabled')

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
    def __init__(self, cloud_directory: CloudDirectory, name: str, local: bool = False):
        """

        :param cloud_directory:
        :param name:
        :param local: use if you don't want to retrieve information from the directory when initializing
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
               statement: typing.Optional[str] = None,
               file_name: typing.Optional[str] = None):
        if file_name:
            with open(file_name, 'r') as fp:
                statement = fp.read()
        if statement:
            object_ref, policy_ref = cloud_directory.create_object(quote(name), statement, 'Group', name=name)
            new_node = cls(cloud_directory, name)
            new_node._statement = statement
            new_node._policy = policy_ref
            return new_node
        raise ValueError("statement and file_name cannot be None.")

    def get_user_names(self, batch_size=30) -> typing.Iterator[str]:
        """
        Retrieves the user names for all user in this group.
        :param batch_size: the max number of results to fetch in a single batch request
        :return:
        """
        end_loop = False
        user_iterator = self.cd.list_object_children(self.object_reference)
        while True:
            operations = []
            try:
                for i in range(batch_size):
                    user = user_iterator.__next__()
                    operations.append(self.cd.batch_get_attributes(user, 'User', ['name']))
            except StopIteration:
                end_loop = True
            for resp in self.cd.batch_read(operations)['Responses']:
                yield resp['SuccessfulResponse']['GetObjectAttributes']['Attributes'][0]['Value']['StringValue']
            if end_loop:
                break

    def get_users(self) -> typing.Iterator[str]:
        """
        Retrieves the object_references for all user in this group.
        :return:
        """
        for user in self.cd.list_object_children(self.object_reference):
            yield user

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
        if len(users):
            operations = [
                self.cd.batch_attach_object(self.object_reference,
                                            i.object_reference,
                                            self._link_formats['group'].format(parent=self._path_name,
                                                                               child=i._path_name))
                for i in users]
            self.cd.batch_write(operations)

    def remove_users(self, users: typing.List[str]) -> None:
        """

        :param users: a list of user names to remove from group
        :return:
        """
        for user in users:
            User(self.cd, user, local=True).remove_groups([self._path_name])


class Role(CloudNode):
    def __init__(self, cloud_directory: CloudDirectory, name: str):
        super(Role, self).__init__(cloud_directory, name, 'Role')

    @classmethod
    def create(cls,
               cloud_directory: CloudDirectory,
               name: str,
               statement: str = ''):
        object_ref, policy_ref = cloud_directory.create_object(quote(name), statement, 'Role', name=name)
        new_node = cls(cloud_directory, name)
        new_node._statement = statement
        new_node._policy = policy_ref
        return new_node

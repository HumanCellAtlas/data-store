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

    @staticmethod
    def get_obj_type_path(obj_type):
        obj_type = obj_type.lower()
        paths = dict(group='/Groups/',
                     index='/Indices/',
                     user='/Users/',
                     policy='/Policies/',
                     role='/Roles/')
        return paths[obj_type]

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


class Role(CloudNode):
    def __init__(self, cloud_directory: CloudDirectory, name: str):
        super(Role, self).__init__(cloud_directory, name, 'Role')

    @classmethod
    def create(cls,
               cloud_directory: CloudDirectory,
               name: str,
               statement: typing.Optional[str] = None):
        if statement:
            _, policy_ref = cloud_directory.create_object(quote(name), statement, 'Role', name=name)
            new_node = cls(cloud_directory, name)
            new_node._statement = statement
            new_node._policy = policy_ref
            return new_node
        raise ValueError("statement and file_name cannot be None.")

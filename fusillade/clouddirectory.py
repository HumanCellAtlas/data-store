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

    def _get_object_attribute_list(self, facet="User", **kwargs):
        return [dict(Key=dict(SchemaArn=self._schema, FacetName=facet, Name=k), Value=dict(StringValue=v))
                for k, v in kwargs.items()]

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

    def get_object_information(self, obj_ref: str):
        return ad.get_object_information(
            DirectoryArn=self._dir_arn,
            ObjectReference={
                'Selector': obj_ref
            },
            ConsistencyLevel='EVENTUAL'
        )

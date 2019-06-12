#!/usr/bin/env python
"""
Check if your schema is matches the latest in AWS.
Optional you can upgrade the published schema to match your local schema
"""
import argparse
import json
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from fusillade.clouddirectory import cd_client, directory_schema_path, get_json_file, project_arn, \
    CloudDirectory

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--major-version", required=True, type=str,
                    help='The major version of the published schema to check/upgrade')
parser.add_argument("--schema-name", required=True, type=str, help='The name of the published schema to check/upgrade')
parser.add_argument("--directory", required=False, default=None, type=str,
                    help='The name of the directory to check/upgrade schema')
parser.add_argument("--upgrade", action="store_true", help='If true the published schema will be upgraded, '
                                                           'else compare it with the local and return results')
args = parser.parse_args()


def update_service_config():
    # upgrade service config
    with open(os.path.join(pkg_root, "service_config.json")) as fh:
        service_config = json.load(fh)
    service_config['directory_schema']["MinorVersion"] = pub_schema_arn.split('/')[-1]
    with open(os.path.join(pkg_root, "service_config.json"), 'w') as fh:
        service_config = json.dump(service_config, fh, indent=4)


name = args.schema_name
version = {'Version': args.major_version, 'MinorVersion': '0'}
schema_outofdate = False
directory_outofdate = False

# open schema file locally
new_schema = get_json_file(directory_schema_path)

# check if published schema exists
published_schemas = cd_client.list_published_schema_arns(
    SchemaArn=f"{project_arn}schema/published/{name}/{version['Version']}",
    MaxResults=30)['SchemaArns']
pub_schema_arn = published_schemas[-1]
try:
    published = cd_client.get_schema_as_json(SchemaArn=pub_schema_arn)['Document']
except cd_client.exceptions.ResourceNotFoundException:
    schema_outofdate = True
else:
    # compare new_schema with published
    new = json.loads(new_schema)
    new.pop('sourceSchemaArn')
    new = json.dumps(new, sort_keys=True)
    old = json.loads(published)
    old.pop('sourceSchemaArn')
    old = json.dumps(old, sort_keys=True)

    if new == old:
        print('Published schema is up to date!')
    else:
        print("Published schema is out of date.")
        schema_outofdate = True

if schema_outofdate and args.upgrade:
    try:
        # create a new development schema
        dev_schema_arn = cd_client.create_schema(Name=name)['SchemaArn']
    except cd_client.exceptions.SchemaAlreadyExistsException:
        # if schema exists use that one
        dev_schema_arn = f"{project_arn}schema/development/{name}"
    # update the dev schema
    cd_client.put_schema_from_json(SchemaArn=dev_schema_arn, Document=new_schema)
    try:
        # publish the schema with a minor version
        new_schema_arn = cd_client.publish_schema(DevelopmentSchemaArn=dev_schema_arn, **version)['PublishedSchemaArn']
    except cd_client.exceptions.SchemaAlreadyPublishedException:
        # if version/minor versions exists upgrade
        minor = max([int(i.split('/')[-1]) for i in published_schemas]) + 1
        pub_schema_arn = cd_client.upgrade_published_schema(
            DevelopmentSchemaArn=dev_schema_arn,
            PublishedSchemaArn=pub_schema_arn,
            MinorVersion=str(minor),
            DryRun=False
        )['UpgradedSchemaArn']

if args.directory:
    directory = CloudDirectory.from_name(args.directory)

    applied_schemas = cd_client.list_applied_schema_arns(DirectoryArn=directory._dir_arn)['SchemaArns']
    print(f"Schemas applied to {args.directory}:")
    for i in applied_schemas:
        print(f"\t {i}")
    applied_schema = [i for i in applied_schemas if args.schema_name in i][0]
    if not applied_schema:
        print('Publish schema has not been applied to directory.')
    else:
        applied_schema = cd_client.get_applied_schema_version(SchemaArn=applied_schema)['AppliedSchemaArn']
        print(f"Current schemas version applied to {args.directory}: {applied_schema}")
        applied_schema_json = cd_client.get_schema_as_json(SchemaArn=applied_schema)['Document']
        # directory_schema_published = get_published_schema_from_directory(directory._dir_arn)

        # compare new_schema with published
        new = json.loads(new_schema)
        new.pop('sourceSchemaArn')
        new = json.dumps(new, sort_keys=True)
        old = json.loads(applied_schema_json)
        old.pop('sourceSchemaArn')
        old = json.dumps(old, sort_keys=True)

        if new == old:
            print('Directory schema is up to date!')
        else:
            print("Directory schema is out of date.")
            directory_outofdate = True

    if directory_outofdate:
        if args.upgrade:
            response = cd_client.upgrade_applied_schema(
                PublishedSchemaArn=pub_schema_arn,
                DirectoryArn=directory._dir_arn,
                DryRun=True | False
            )
            print(f"Directory Schema Upgraded from {applied_schema} to {pub_schema_arn}")
        else:
            response = cd_client.upgrade_applied_schema(
                PublishedSchemaArn=pub_schema_arn,
                DirectoryArn=directory._dir_arn,
                DryRun=True
            )
            print(f"Directory Schema upgrade compatible from {applied_schema} to {pub_schema_arn}")
        update_service_config()

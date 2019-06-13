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
parser.add_argument("--stage", required=False, default=os.getenv('FUS_DEPLOYMENT_STAGE'), type=str,
                    help='The deployment stage to check.')
parser.add_argument("--upgrade-directory", action="store_true",
                    help="If set, the directory will be updated to the latest published schema.")
parser.add_argument("--upgrade-published", action="store_true",
                    help="If set, the local schema will update the published schema.")
args = parser.parse_args()

if not args.stage:
    print("'FUS_DEPLOYMENT_STAGE' not found in environment please run `source environment`, or use the --stage option")
    exit(1)
new_schema = get_json_file(directory_schema_path)  # open schema file locally
schema_name = f"hca_fusillade_base_{args.stage}"
directory_name = f"hca_fusillade_{args.stage}"
rv = 0

def update_service_config(schema_arn):
    # upgrade service config
    with open(os.path.join(pkg_root, "service_config.json")) as fh:
        service_config = json.load(fh)
    service_config['directory_schema']["MinorVersion"] = schema_arn.split('/')[-1]
    service_config['directory_schema']["Version"] = schema_arn.split('/')[-2]
    with open(os.path.join(pkg_root, "service_config.json"), 'w') as fh:
        json.dump(service_config, fh, indent=4)


def schemas_equal(a, b):
    a = json.loads(a)
    a.pop('sourceSchemaArn')
    a = json.dumps(a, sort_keys=True)
    b = json.loads(b)
    b.pop('sourceSchemaArn')
    b = json.dumps(b, sort_keys=True)
    return a == b


def get_published(major):
    # check if published schema exists
    _schema_arn = f"{project_arn}schema/published/{schema_name}/{major}"
    return cd_client.list_published_schema_arns(
        SchemaArn=_schema_arn)['SchemaArns']


def update_dev_schema(schema):
    try:
        # create a new development schema
        dev_schema_arn = cd_client.create_schema(Name=schema_name)['SchemaArn']
    except cd_client.exceptions.SchemaAlreadyExistsException:
        # if schema exists use that one
        dev_schema_arn = f"{project_arn}schema/development/{schema_name}"

    # update the dev schema
    cd_client.put_schema_from_json(SchemaArn=dev_schema_arn, Document=schema)
    return dev_schema_arn


directory = CloudDirectory.from_name(directory_name)
applied_schemas = cd_client.list_applied_schema_arns(DirectoryArn=directory._dir_arn)['SchemaArns']

print(f"Schemas applied to directory {directory_name}:")
for i in applied_schemas:
    print(f"\t- {i}")

applied_schema = [i for i in applied_schemas if schema_name in i][0]
if not applied_schema:
    #  This should never happen
    print('Publish schema has not been applied to directory. Check Cloud Directory\n')
    exit(1)

applied_schema = cd_client.get_applied_schema_version(SchemaArn=applied_schema)['AppliedSchemaArn']
print(f"Current schemas version applied to directory {directory_name}:\n\t- {applied_schema}\n")

#  Get version info from schema arn
major_version, minor_version = applied_schema.split('/')[-2:]
if major_version == schema_name:  # Check if their is a minor version by parsing the schema arn.
    # No minor version so set major version to the parsed minor_version.
    major_version = minor_version
    minor_version = '0'


# --- Check if the directory is up to date with the most recent published schema.
pub_schemas = get_published(major_version)
pub_schema_arn_latest = pub_schemas[-1]
pub_schema_json = cd_client.get_schema_as_json(SchemaArn=pub_schema_arn_latest)['Document']
applied_schema_json = cd_client.get_schema_as_json(SchemaArn=applied_schema)['Document']
directory_uptodate = schemas_equal(pub_schema_json, applied_schema_json)
if directory_uptodate:
    print('Directory schema is up to date!')
else:
    print("Directory schema is out of date.")
    print('-' * 16)
    print(f"Published:\n{json.dumps(json.loads(pub_schema_json), indent=2)}")
    print('-' * 16)
    print(f"Applied:\n{json.dumps(json.loads(applied_schema_json), indent=2)}")
    print('-' * 16)

# --- Check if the schema is up to date with the local schema
schema_uptodate = schemas_equal(new_schema, pub_schema_json)
if schema_uptodate:
    print(f"Published schema {pub_schema_arn_latest} is up to date.")
else:
    print(f"Published schema {pub_schema_arn_latest} is out of date!")
    print('-' * 16)
    print(f"Local:\n{json.dumps(json.loads(new_schema), indent=2)}")
    print('-' * 16)
    print(f"Published:\n{json.dumps(json.loads(pub_schema_json), indent=2)}")
    print('-' * 16)

    dev_schema_arn = update_dev_schema(new_schema)
    minor = max([int(i.split('/')[-1]) for i in pub_schemas]) + 1
    if args.upgrade_published:
        pub_schema_arn_latest = cd_client.upgrade_published_schema(
            DevelopmentSchemaArn=dev_schema_arn,
            PublishedSchemaArn=pub_schema_arn_latest,
            MinorVersion=str(minor),
            DryRun=False
        )['UpgradedSchemaArn']
    else:
        pub_schema_arn_latest = cd_client.upgrade_published_schema(
            DevelopmentSchemaArn=dev_schema_arn,
            PublishedSchemaArn=pub_schema_arn_latest,
            MinorVersion=str(minor),
            DryRun=True
        )
        print(f"Published Schema upgrade compatible from {pub_schemas[-1]} to {pub_schema_arn_latest}. Please "
              f"upgrade.")
        rv = 1

if not directory_uptodate:
    if args.upgrade_directory:
        print("Upgrading directory schema to {pub_schema_arn}")
        response = cd_client.upgrade_applied_schema(
            PublishedSchemaArn=pub_schema_arn_latest,
            DirectoryArn=directory._dir_arn,
            DryRun=False
        )
        print(f"Directory Schema Upgraded from {applied_schema} to {pub_schema_arn_latest}")
        update_service_config(pub_schema_arn_latest)
    else:
        response = cd_client.upgrade_applied_schema(
            PublishedSchemaArn=pub_schema_arn_latest,
            DirectoryArn=directory._dir_arn,
            DryRun=True
        )
        print(f"Directory Schema upgrade compatible from {applied_schema} to {pub_schema_arn_latest}. Please upgrade.")
        rv = 1
exit(rv)

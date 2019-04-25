import boto3
from fusillade.clouddirectory import cleanup_directory, cleanup_schema
"""
This script is used to clean up test directories and schemas from aws clouddirectory
"""

client = boto3.client("clouddirectory")

response = client.list_directories(
    MaxResults=30,
    state='ENABLED'
)

for directory in response['Directories']:
    if 'test' in directory['Name']:
        cleanup_directory['DirectoryArn']

directories = [ i['Name'] for i in client.list_directories(
    MaxResults=30,
    state='ENABLED'
)['Directories']]
print('DIRECTORIES:', directories)

response = client.list_published_schema_arns(
    MaxResults = 20
)
for schema in response['SchemaArns']:
    if "authz/T" in schema:
        cleanup_schema(schema)

response = client.list_published_schema_arns(
    MaxResults = 20
)['SchemaArns']
print('Schemas:',response)
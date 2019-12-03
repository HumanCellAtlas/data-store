#!/usr/bin/env python
"""
Assemble and deploy AWS DynamoDB tables (for keeping track of runs and run output) for scale tests
"""
import boto3

dynamodb_client = boto3.client('dynamodb')

SCALABILITY_TEST_TABLE = 'scalability_test'
SCALABILITY_TEST_RUN_TABLE = 'scalability_test_result'

existing_tables = dynamodb_client.list_tables()['TableNames']
if SCALABILITY_TEST_TABLE not in existing_tables:
    dynamodb_client.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'execution_id',
                'AttributeType': 'S',
            },
            {
                'AttributeName': 'run_id',
                'AttributeType': 'S',
            }
        ],
        KeySchema=[
            {
                'AttributeName': 'execution_id',
                'KeyType': 'HASH',
            },
            {
                'AttributeName': 'run_id',
                'KeyType': 'RANGE',
            },
        ],
        StreamSpecification={
            'StreamEnabled': True,
            'StreamViewType': 'NEW_IMAGE'
        },
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5,
        },
        TableName=SCALABILITY_TEST_TABLE,
    )

    waiter = dynamodb_client.get_waiter('table_exists')
    waiter.wait(TableName=SCALABILITY_TEST_TABLE)

    dynamodb_client.update_time_to_live(
        TableName=SCALABILITY_TEST_TABLE,
        TimeToLiveSpecification={
            'Enabled': True,
            'AttributeName': 'expiration_ttl'
        }
    )

if SCALABILITY_TEST_RUN_TABLE not in existing_tables:
    dynamodb_client.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'run_id',
                'AttributeType': 'S',
            }
        ],
        KeySchema=[
            {
                'AttributeName': 'run_id',
                'KeyType': 'HASH',
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5,
        },
        TableName=SCALABILITY_TEST_RUN_TABLE,
    )

    waiter = dynamodb_client.get_waiter('table_exists')
    waiter.wait(TableName=SCALABILITY_TEST_RUN_TABLE)

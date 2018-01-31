import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    success_count = 0
    failure_count = 0
    records = 0
    duration_sum = 0
    run_id = None
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            run_id = record['dynamodb']['NewImage']['run_id']['S']
            status = record['dynamodb']['NewImage']['status']['S']
            duration = record['dynamodb']['NewImage']['duration']['N']
            duration_sum += Decimal(duration)
            records += 1
            if status == 'SUCCEEDED':
                success_count += 1
            else:
                failure_count += 1
    if records > 0:
        table = dynamodb.Table('scalability_test_run')
        run_entry_pk = {'run_id': run_id}
        run_entry = table.get_item(Key=run_entry_pk)
        if run_entry.get('Item'):
            old_avg_duration = run_entry['Item']['average_duration']
            old_count = run_entry['Item']['succeeded_count'] + run_entry['Item']['failed_count']

            table.update_item(
                Key=run_entry_pk,
                UpdateExpression='SET '
                                 'succeeded_count = succeeded_count + :success_count, '
                                 'failed_count = failed_count + :failure_count, '
                                 'average_duration = :new_average',
                ExpressionAttributeValues={
                    ':success_count': success_count,
                    ':failure_count': failure_count,
                    ':new_average': old_avg_duration + (duration_sum / records - old_avg_duration) /
                                                       (old_count + records)
                }
            )
        else:
            print('No run entries found')
    else:
        print('No INSERT records to process')

    return 'Successfully processed {} records.'.format(len(event['Records']))

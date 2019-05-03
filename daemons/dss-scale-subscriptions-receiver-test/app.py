"""
This lambda/chalice app serves as a callback url endpoint for subscriptions for scale testing.

For example:
If 1,000 subscriptions are created for this endpoint (triggered by bundle creation), and
1,000 bundles are created, then this app should receive 1,000,000 notifications.

The metrics will be timed and logged.

TODO: Log to dynamoDB instead of writing log files to a bucket.
TODO: Integrate with dss-scalability-test.
"""
from chalice import Chalice
from requests_http_signature import HTTPSignatureAuth
from requests import Request
import time
import boto3
import json
import subprocess


bucket = 'dss-scale-notifications-test'
app = Chalice(app_name='dss-scale-test-notification-receiver')
client = boto3.client('s3')


def get_hmac_key():
    secret_name = 'dcp/dss/dev/scale-test/hmacsecretkey'
    p = subprocess.Popen(f'aws secretsmanager get-secret-value --secret-id {secret_name}',
                         shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    try:
        hmac_key = json.loads(json.loads(stdout.decode('utf-8'))['SecretString'])['secret']
    except:
        raise RuntimeError(f"An error occured:\n{stderr.decode('utf-8')}")
    return hmac_key


hmac_key = get_hmac_key()


def log_to_bucket(body, key, retries=[1, 2, 4, 8, 16, 32, 64]):
    try:
        client.put_object(Body=body, Bucket=bucket, Key=key)
    except:  # noqa
        if retries:
            time.sleep(retries.pop(0))
            log_to_bucket(body, key, retries=retries)
        else:
            raise


@app.route('/notification', methods=['GET', 'POST'])
def subscription_notification():
    request = app.current_request

    time_sent = time.time().split('.')[0]  # whole seconds will do

    try:
        HTTPSignatureAuth.verify(Request(request.method, request.path, request.headers),
                                 key_resolver=lambda key_id, algorithm: hmac_key.encode())
        response = request.data.json()

        # can be used to ID who sent the notification
        # so if the scale test creates 5000 bundles, all will have the same version and others can be filtered out
        version = response['match']['bundle_version'].replace('.', '')

        # filter to ensure all were hit; should show 5000 times per subscription if 5000 bundles created
        subscription_id = response['subscription_id'].replace('.', '')

        # unique each time
        bundle = response['match']['bundle_uuid'].replace('.', '')
        log_to_bucket(body='', key='.'.join([time_sent, version, subscription_id, bundle]))

    except Exception as e:
        log_to_bucket(body=f'{str(e)}!', key='.'.join([time_sent, 'error']))
        return app.make_response(f'{str(e)}!\n')

    return 'Subscription Notification Successful!\n'


@app.route('/', methods=['GET', 'POST'])
def root_method():
    return 'Test.\n'

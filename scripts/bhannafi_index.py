#!/usr/bin/env python
"""
Trigger the indexer by injecting object create SNS events
"""
import os
import json
import time
import argparse

from bhannafi_utils import resources, clients, get_bucket


PARALLEL_FACTOR=40


def topic_arn(topic):
    region = "us-east-1"
    account_id = clients.sts.get_caller_identity()['Account']
    return f"arn:aws:sns:{region}:{account_id}:{topic}"


def send_sns_msg(topic_arn, message, attributes=None): 
    sns_topic = resources.sns.Topic(str(topic_arn)) 
    args = {'Message': json.dumps(message)} 
    if attributes is not None:
        args['MessageAttributes'] = attributes
    sns_topic.publish(**args)


def index_gcp(bucket, key):
    data = {
        'bucket': bucket,
        'name': key,
    }
    topic = f"dss-gs-bucket-events-{bucket}"
    send_sns_msg(topic_arn(topic), data)


def index_aws(bucket, key):
    data = {
        'Records': [{
            's3': {
                'bucket': {
                    'name': bucket
                },
                'object': {
                    'key': key
                },
            }
        }]
    }
    topic = f"domovoi-s3-bucket-events-{bucket}"
    send_sns_msg(topic_arn(topic), data)


def index(replica, bucket, key):
    if "aws" == replica:
        index_aws(bucket, key)
    elif "gcp" == replica:
        index_gcp(bucket, key)
    else:
        raise Exception(f"unknown replica {replica}")
    print(f"Sending index SNS for {replica} {bucket} {key}")


def index_with_keys(replica, bucket, keys):
    for key in keys:
        index(replica, bucket, key)
        time.sleep(1)
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("replica", choices=["aws", "gcp"])
    parser.add_argument("key_or_file")
    parser.add_argument("-d", "--stage", default="dev", choices=["dev", "integration", "staging", "prod"])
    args = parser.parse_args()

    bucket = get_bucket(args.stage, args.replica)
    
    if os.path.isfile(args.key_or_file):
        with open(args.key_or_file, "r") as fh:
            keys = [line.strip() for line in fh]
        index_with_keys(args.replica, bucket, keys)
    else:
        index(args.replica, bucket, args.key_or_file)


import os
import boto3
from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket


class BlobListerizer:

    def __init__(self, replica, bucket, prefix, marker=None, token=None):

        self.replica = replica
        self.bucket = bucket
        self.prefix = prefix
        self.marker = marker
        self.token = token
        self.k_page_max = 2


    def iter_aws(self):

        client = boto3.client('s3')
    
        kwargs = {
            'Bucket': self.bucket,
            'Prefix': self.prefix,
            'MaxKeys': self.k_page_max,
        }
    
        while True:
    
            if self.token:
                kwargs['ContinuationToken'] = self.token

            resp = client.list_objects_v2(
                ** kwargs
            )
    
            if resp['IsTruncated']:
                self.token = resp['NextContinuationToken']
            else:
                self.token = None

            if resp.get('Contents', None):
                contents = resp['Contents']
            else:
                contents = list()
    
            i = 0
            if self.marker:
                try:
                    i = 1 + next(i for (i,d) in enumerate(contents) if d['Key'] == self.marker)
                    contents = contents[i:]
                except StopIteration:
                    pass
    
            for d in contents:
                self.marker = d['Key']
                yield self.marker
            else:
                self.marker = None

            if not self.token:
                break


    def iter_gcp(self):

        client = Client.from_service_account_json(
            os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        )
    
        kwargs = {
            'prefix': self.prefix,
            'max_results': self.k_page_max,
        }
    
        while True:
    
            if self.token:
                kwargs['page_token'] = self.token

            resp = client.bucket(self.bucket).list_blobs(
                ** kwargs
            )
            contents = list(resp)
            self.token = resp.next_page_token
    
            i = 0
            if self.marker:
                try:
                    i = 1 + next(i for (i,d) in enumerate(contents) if d.name == self.marker)
                    contents = contents[i:]
                except StopIteration:
                    pass
    
            for d in contents:
                self.marker = d.name
                yield self.marker
            else:
                self.marker = None

            if not self.token:
                break

    def __iter__(self):
        if 'aws' == self.replica:
            it = self.iter_aws()
        elif 'gcp' == self.replica:
            it = self.iter_gcp()
        else:
            raise NotImplementedError

        return it

#!/usr/bin/env python

"""
This script configures a regional domain name for API Gateway, based on an existing
certificate, and Route 53 zone. The certificate must be in the same region as the regional
domain. 
"""

import boto3
import argparse

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--domain-name", required=True)
parser.add_argument("--certificate-arn", required=True)
parser.add_argument("--zone-id", required=True)
args = parser.parse_args()

resp = boto3.client("apigateway").create_domain_name(
    domainName=args.domain_name,
    regionalCertificateArn=args.certificate_arn,
    endpointConfiguration={'types': ["REGIONAL"]}
)

boto3.client("route53").change_resource_record_sets(
    HostedZoneId=args.zone_id,
    ChangeBatch={
        'Changes': [{
            'Action': 'CREATE',
            'ResourceRecordSet': {
                'Name': f"{args.domain_name}.",
                'Type': 'CNAME',
                'ResourceRecords': [ {'Value': resp['regionalDomainName']} ],
                'TTL': 300,
            }
        }]
    }
)

#!/usr/bin/env python
"""
Grab the list of access ips from the ES cluster and put them in Secretsmanager with name `es_source_ip`.
"""
import os
import re
import json
import boto3
import subprocess


es_client = boto3.client('es')


domain = es_client.describe_elasticsearch_domains(
    DomainNames=[
        os.environ['DSS_ES_DOMAIN'],
    ]
)['DomainStatusList'][0]
policy = json.loads(domain['AccessPolicies'])

for statement in policy['Statement']:
    if 'Condition' in statement:
        ips = ",".join(statement['Condition']['IpAddress']['aws:SourceIp'])
        subprocess.run(
            ["scripts/dss-ops.py", "secrets", "set", "es_source_ip", "--force"],
            input=ips.encode("utf-8")
        )

#!/usr/bin/env python
"""
This file compiles $EXPORT_ENV_VARS_TO_LAMBDA into a json document and
uploads it into AWS Systems Manager Parameter Store under the key
`dcp/dss/{DSS_DEPLOYMENT_STAGE}/environment`
"""
import os
import json
import boto3


ssm_client = boto3.client("ssm")
es_client = boto3.client("es")

dss_es_domain = es_client.describe_elasticsearch_domain(
    DomainName=os.environ['DSS_ES_DOMAIN']
)

parms = {var: os.environ[var]
         for var in os.environ['EXPORT_ENV_VARS_TO_LAMBDA'].split()}
parms['DSS_ES_ENDPOINT'] = dss_es_domain['DomainStatus']['Endpoint']

ssm_client.put_parameter(
    Name=f"/dcp/dss/{os.environ['DSS_DEPLOYMENT_STAGE']}/environment",
    Value=json.dumps(parms),
    Type="String",
    Overwrite=True,
)

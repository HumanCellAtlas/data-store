{
  "terraform": {
    "backend": {
      "s3": {
        "bucket": "org-humancellatlas-config-prod",
        "key": "dss-domain-prod.tfstate",
        "region": "us-east-1",
        "profile": "hca-id",
        "role_arn": "arn:aws:iam::109067257620:role/dcp-admin"
      }
    }
  }
}

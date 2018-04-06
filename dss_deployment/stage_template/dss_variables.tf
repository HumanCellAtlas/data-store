{
  "variable": {
    "DSS_DEPLOYMENT_STAGE": {
      "description": "Name of deployment.",
      "default": null
    },
    "AWS_DEFAULT_REGION": {
      "description": "AWS deployment region.",
      "default": "us-east-1"
    },
    "GCP_DEFAULT_REGION": {
      "description": "Google infrastructure default region.",
      "default": "us-central1"
    },
    "DSS_S3_BUCKET": {
      "description": "DSS S3 Bucket.",
      "default": "dss-{stage}"
    },
    "DSS_S3_BUCKET_TEST": {
      "description": "S3 test bucket. Enter \"none\" if you do not intend to run tests.",
      "default": "dss-test-{stage}"
    },
    "DSS_S3_BUCKET_TEST_FIXTURES": {
      "description": "S3 test fixtures bucket. Enter \"none\" if you do not intend to run tests.",
      "default": "dss-test-fixtures-{stage}"
    },
    "DSS_S3_CHECKOUT_BUCKET": {
      "description": "S3 checkout service bucket.",
      "default": "dss-checkout-{stage}"
    },
    "DSS_S3_CHECKOUT_BUCKET_TEST": {
      "description": "S3 checkout service test bucket. Enter \"none\" if you do not intend to run tests.",
      "default": "dss-checkout-test-{stage}"
    },
    "DSS_S3_CHECKOUT_BUCKET_TEST_FIXTURES": {
      "description": "S3 checkout service test fixtures bucket. Enter \"none\" if you do not intend to run tests.",
      "default": "dss-checkout-test-fixtures-{stage}"
    },
    "DSS_GS_BUCKET": {
      "description": "DSS google bucket.",
      "default": "dss-{stage}"
    },
    "DSS_GS_BUCKET_TEST": {
      "description": "DSS google test bucket. Enter \"none\" if you do not intend to run tests.",
      "default": "dss-test-{stage}"
    },
    "DSS_GS_BUCKET_TEST_FIXTURES": {
      "description": "DSS google test fixtures bucket. Enter \"none\" if you do not intend to run tests.",
      "default": "dss-test-fixtures-{stage}"
    },
    "DSS_GS_CHECKOUT_BUCKET": {
      "description": "GS checkout service bucket.",
      "default": "dss-checkout-{stage}"
    },
    "DSS_GS_CHECKOUT_BUCKET_TEST": {
      "description": "GS checkout service test bucket. Enter \"none\" if you do not intend to run tests.",
      "default": "dss-checkout-test-{stage}"
    },
    "DSS_GS_CHECKOUT_BUCKET_TEST_FIXTURES": {
      "description": "GS checkout service test fixtures bucket. Enter \"none\" if you do not intend to run tests.",
      "default": "dss-checkout-test-fixtures-{stage}"
    },
    "DSS_ES_DOMAIN": {
      "description": "Elasticsearch domain name.",
      "default": "dss-index-{stage}"
    },
    "API_DOMAIN_NAME": {
      "description": "Domain name of your deployment (e.g. dss.dev.data.humancellatlas.org).",
      "default": null
    },
    "DSS_PARAMETER_STORE": {
      "description": "Name of AWS SSM parameter store used to keep event relay credentials.",
      "default": "/dss/parameters"
    },
    "DSS_EVENT_RELAY_AWS_USERNAME": {
      "description": "AWS IAM user providing identity to the event relay",
      "default": "dss-event-relay"
    },
    "DSS_EVENT_RELAY_AWS_ACCESS_KEY_ID_PARAMETER_NAME": {
      "description": "Event relay IAM user access key id parameter name",
      "default": "event_relay_aws_access_key_id"
    },
    "DSS_EVENT_RELAY_AWS_SECRET_ACCESS_KEY_PARAMETER_NAME": {
      "description": "Event relay IAM user secret access key parameter name",
      "default": "event_relay_aws_secret_access_key"
    }
  }
}

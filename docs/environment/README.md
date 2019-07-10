Environment Variable | Description
--- | ---
`ACM_CERTIFICATE_IDENTIFIER` | The AWS ACM certificate identifier, which can be found on the AWS console.
`ADMIN_USER_EMAILS_SECRETS_NAME` | The name of the secret stored in AWS Secrets Manager.
`ADMIN_USER_EMAILS` |
`API_DOMAIN_NAME` | The domain of the API host.
`AWS_DEFAULT_OUTPUT` | The default output format for AWS CLI commands.
`AWS_DEFAULT_REGION` | The default region that the AWS CLI and boto3 will use.
`AWS_SDK_LOAD_CONFIG` | Needed for Terraform to correctly use AWS assumed roles
`CHECKOUT_CACHE_CRITERIA` | Specifies which files are cached based on content-type and size.
`DCP_DOMAIN` | The domain name of the DSS.
`DSS_AUTHORIZED_DOMAINS_TEST` | A domain to be used during testing.
`DSS_AUTHORIZED_DOMAINS` | Populated from `DSS_AUTHORIZED_GOOGLE_PROJECT_DOMAIN_ARRAY`.
`DSS_AUTHORIZED_GOOGLE_PROJECT_DOMAIN_ARRAY` | A list domains with authorization to perform restricted actions on the DSS. This is generally used for Google service account credentials.
`DSS_BLOB_TTL_DAYS` | The time to live of an object in cloud storage enforced by the cloud provider's bucket lifecycle poilcy.
`DSS_CHECKOUT_BUCKET_OBJECT_VIEWERS` | This list manages the GCP Users and serviceAccounts able to access direct URLs on the GS checkout bucket. Other GCP entities must use presigned urls, or checkout to an external GS bucket they have access to.
`DSS_DEBUG` | An interger specifying the log level. See [dss.logging](https://github.com/HumanCellAtlas/data-store/blob/c61e2cf000bf64e54a572f5dc29807feb8ee34c6/dss/logging.py#L31).
`DSS_DEPLOYMENT_STAGE` | The name of the DSS deployment. This value should be appended to all bucket and step function names. The Human Cell Atlas project maintains for deployment stages: `dev`, `integration`, `staging` and `prod`.
`DSS_ES_DOMAIN` | The name of the AWS Elasticsearch domain.
`DSS_ES_INSTANCE_COUNT` | The number of nodes in the AWS Elasticsearch cluster.
`DSS_ES_INSTANCE_TYPE` | The type of AWS Elasticsearch instance>
`DSS_ES_VOLUME_SIZE` | The total storage size dedicated to the AWS Elasticsearch cluster.
`DSS_GCP_SERVICE_ACCOUNT_NAME` | "travis-test"
`DSS_GS_BUCKET_INTEGRATION` | The name of the HCA DSS' main bucket in GS on the HCA integration environment.
`DSS_GS_BUCKET_PROD` | The name of the HCA DSS' main bucket in GS on the HCA production environment.
`DSS_GS_BUCKET_REGION` | Workaround for GS buckets with non-uniform regions
`DSS_GS_BUCKET_STAGING` | The name of the HCA DSS' main bucket in GS on the HCA staging environment.
`DSS_GS_BUCKET_TEST_FIXTURES_REGION` | Workaround for GS buckets with non-uniform regions
`DSS_GS_BUCKET_TEST_FIXTURES` | The name of the DSS' test fixtures bucket in GS. This bucket stores static test data such as bundles and files that are required to run unit and integration tests. Test fixture buckets can be populated by running the script: `tests/fixtures/populate.py --s3-bucket $DSS_S3_BUCKET_TEST_FIXTURES --gs-bucket $DSS_GS_BUCKET_TEST_FIXTURES`.
`DSS_GS_BUCKET_TEST_REGION` | Workaround for GS buckets with non-uniform regions
`DSS_GS_BUCKET_TEST` | The name of the DSS' test bucket in GS. This bucket replaces `DSS_S3_BUCKET` during unit and integration tests. The bucket name should be suffixed with `DSS_DEPLOYMENT_STAGE`.
`DSS_GS_BUCKET` | The name of the DSS' main bucket in GS. This bucket stores file contents in `blobs/`, file metadata in ` files/` and bundle manifests in `bundles/`. The bucket name should be suffixed with `DSS_DEPLOYMENT_STAGE`.
`DSS_GS_CHECKOUT_BUCKET_INTEGRATION` | The name of the HCA DSS' checkout bucket in GS on the HCA integration environment.
`DSS_GS_CHECKOUT_BUCKET_PROD` | The name of the HCA DSS' checkout bucket in GS on the HCA prod environment.
`DSS_GS_CHECKOUT_BUCKET_STAGING` | The name of the HCA DSS' checkout bucket in GS on the HCA staging environment.
`DSS_GS_CHECKOUT_BUCKET_TEST_USER` | GS bucket representing a non-dss managed user bucket (i.e. testing POST /bundles/{uuid}/checkout)
`DSS_GS_CHECKOUT_BUCKET_TEST` | The name of the DSS' test checkout bucket in GS. This bucket replaces `DSS_S3_CHECKOUT_BUCKET` during unit and integration tests. This bucket name should be suffixed with `DSS_DEPLOYMENT_STAGE`.
`DSS_GS_CHECKOUT_BUCKET` | The name of the DSS' checkout bucket in GS. On `GET` during the checkout process copies files from `DSS_S3_BUCKET` to this bucket. This bucket name should be suffixed with `DSS_DEPLOYMENT_STAGE`.
`DSS_MONITOR_WEBHOOK_SECRET_NAME`	| Webhook URL to post metrics notifications from the DSS Monitor Fargate Task
`DSS_NOTIFICATION_SENDER` |
`DSS_NOTIFY_DELAYS` | Configure the delays between notification attempts. The first attempt is immediate. The second attempt is one minute later. Then ten minutes, one hour, and six hours in between. Then 24 hours minus all previous delays and lastly every 24 hours for six days. This is exponential initially and then levels off where exponential would be too infrequent. The last attempt is made seven days after the first one, which is easy to remember.
`DSS_NOTIFY_TIMEOUT` | This may seem excessive but the default of 10s is not enough for Green's Lira and would cause the notification to be retried, causing Lira to make a duplicate submission.
`DSS_PARAMETER_STORE` | The prefix for DSS parameters stored in AWS Systems Manager.
`DSS_S3_BUCKET_INTEGRATION` | The name of the HCA DSS' main bucket in S3 on the HCA integration environment.
`DSS_S3_BUCKET_PROD` | The name of the HCA DSS' main bucket in S3 on the HCA production environment.
`DSS_S3_BUCKET_STAGING` | The name of the HCA DSS' main bucket in S3 on the HCA staging environment.
`DSS_S3_BUCKET_TEST_FIXTURES` | The name of the DSS' test fixtures bucket in S3. This bucket stores static test data such as bundles and files that are required to run unit and integration tests. Test fixture buckets can be populated by running the script: `tests/fixtures/populate.py --s3-bucket $DSS_S3_BUCKET_TEST_FIXTURES --gs-bucket $DSS_GS_BUCKET_TEST_FIXTURES`.
`DSS_S3_BUCKET_TEST` | The name of the DSS' test bucket in S3. This bucket replaces `DSS_S3_BUCKET` during unit and integration tests. The bucket name should be suffixed with `DSS_DEPLOYMENT_STAGE`.
`DSS_S3_BUCKET` | The name of the DSS' main bucket in S3. This bucket stores file contents in `blobs/`, file metadata in ` files/` and bundle manifests in `bundles/`. The bucket name should be suffixed with `DSS_DEPLOYMENT_STAGE`.
`DSS_S3_CHECKOUT_BUCKET_INTEGRATION` | The name of the HCA DSS' checkout bucket in S3 on the HCA integration environment.
`DSS_S3_CHECKOUT_BUCKET_PROD` | The name of the HCA DSS' checkout bucket in S3 on the HCA prod environment.
`DSS_S3_CHECKOUT_BUCKET_STAGING` | The name of the HCA DSS' checkout bucket in S3 on the HCA staging environment.
`DSS_S3_CHECKOUT_BUCKET_TEST_USER` | S3 bucket representing a non-dss managed user bucket (i.e. testing POST /bundles/{uuid}/checkout)
`DSS_S3_CHECKOUT_BUCKET_TEST` | The name of the DSS' test checkout bucket in S3. This bucket replaces `DSS_S3_CHECKOUT_BUCKET` during unit and integration tests. This bucket name should be suffixed with `DSS_DEPLOYMENT_STAGE`.
`DSS_S3_CHECKOUT_BUCKET_UNWRITABLE` | The name of the DSS' unwritable checkout bucket. This bucket is used for testing purposes.
`DSS_S3_CHECKOUT_BUCKET` | The name of the DSS' checkout bucket in S3. On `GET` during the checkout process copies files from `DSS_S3_BUCKET` to this bucket. This bucket name should be suffixed with `DSS_DEPLOYMENT_STAGE`.
`DSS_SECRETS_STORE` | The prefix for DSS secrets stored in AWS Secrets Manager.
`DSS_TERRAFORM_BACKEND_BUCKET_TEMPLATE` | "dss-config-{account_id}" - `{account_id}`, if present, will be replaced with the account ID associated with the AWS credentials used for deployment. It can be safely omitted.
`DSS_XRAY_TRACE` | Enables X-Ray profiling of daemons running in AWS Lambdas. A value of 0 disables profiling. A value >1 will enable profiling.
`DSS_ZONE_NAME` | Name of the route53 zone containing the domain name.
`ES_ALLOWED_SOURCE_IP_SECRETS_NAME` | Source IP access list for the AWS Elasticsearch cluster. This should be a comma seperated list of IPs.
`EVENT_RELAY_AWS_ACCESS_KEY_SECRETS_NAME` | The name of the secret stored in AWS Secrets Manager.
`EVENT_RELAY_AWS_USERNAME` | The name of the AWS IAM user authorized for the GCP->AWS event relay.
`EXPORT_ENV_VARS_TO_LAMBDA` | Environment variables that will be exported to daemons on deploy. See `build_deploy_config.sh`.
`GCP_DEFAULT_REGION` | The default region that `gcloud` and `google-cloud-python` will use.
`GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME` | The name of the secret stored in AWS Secrets Manager.
`GOOGLE_APPLICATION_SECRETS_SECRETS_NAME` | The name of the secret stored in AWS Secrets Manager.
`NOTIFY_URL` |
`OIDC_AUDIENCE` | A list of allowed audiences in the JWT.
`OIDC_EMAIL_CLAIM` | The OIDC claim that specifies the users email.
`OIDC_GROUP_CLAIM` | The OIDC claim that specifies the Groups the users belongs to.
`OPENID_PROVIDER` |
`PYTHONWARNINGS` |
`TOKENINFO_FUNC` | Do Not Modify. Used by connexion to verify the JWT in the authorization header of an authenticated request.
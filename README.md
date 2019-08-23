# HCA DSS: The Human Cell Atlas Data Storage System

--- 

[![](https://img.shields.io/badge/slack-%23data--store-557EBF.svg)](https://humancellatlas.slack.com/messages/data-store/)
[![Build Status](https://travis-ci.com/HumanCellAtlas/data-store.svg?branch=master)](https://travis-ci.com/HumanCellAtlas/data-store)
[![codecov](https://codecov.io/gh/HumanCellAtlas/data-store/branch/master/graph/badge.svg)](https://codecov.io/gh/HumanCellAtlas/data-store)

This repository maintains the data storage system of the
[Human Cell Atlas](https://www.humancellatlas.org/). We use this
[Google Drive folder](https://drive.google.com/open?id=0B-_4IWxXwazQbWE5YmtqUWx3RVE) for design docs and
meeting notes, and [this Zenhub board](https://app.zenhub.com/workspace/o/humancellatlas/data-store) to track our GitHub work.

## Overview

---

The DSS is a replicated data storage system designed for hosting large sets of scientific experimental data on
[Amazon S3](https://aws.amazon.com/s3/) and [Google Storage](https://cloud.google.com/storage/). The DSS exposes an API
for interacting with the data and is built using [Chalice](https://github.com/aws/chalice),
[API Gateway](https://aws.amazon.com/api-gateway/) and [AWS Lambda](https://aws.amazon.com/lambda/). The API also
implements [Step Functions](https://aws.amazon.com/step-functions/) to orchestrate Lambdas for long-running tasks such
as large file writes. You can find the API documentation and give it a try [here](https://dss.data.humancellatlas.org/).

#### [Architectural Diagram](https://www.lucidchart.com/documents/view/b65c8898-46e3-4560-b3b2-9e85f1c0a4c7)

![DSS Sync SFN diagram](https://www.lucidchart.com/publicSegments/view/43dfe33a-47c9-466b-9cb6-6d941a406d8f/image.png)

#### DSS API

The DSS API uses [Swagger](http://swagger.io/) to define the [API specification](dss-api.yml) according to the
[OpenAPI 2.0 specification](https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md).
[Connexion](https://github.com/zalando/connexion) is used to map the API specification to its implementation in Python.

You can use the
[Swagger Editor](http://editor.swagger.io/#/?import=https://raw.githubusercontent.com/HumanCellAtlas/data-store/master/dss-api.yml)
to review and edit the API specification. When the API is live, the spec is also available at `/v1/swagger.json`.

## Table of Contents

   * [HCA DSS: The Human Cell Atlas Data Storage System](#hca-dss-the-human-cell-atlas-data-storage-system)
      * [Overview](#overview)
        * [DSS API](#dss-api)
      * [Table of Contents](#table-of-contents)
      * [Getting Started](#getting-started)
        * [Install Dependencies](#install-dependencies)
        * [Configuration](#configuration)
          * [Configure Terraform](#configure-terraform)
          * [Configure AWS](#configure-aws)
          * [Configure GCP](#configure-gcp)
          * [Configure User Authentication/Authorization](#configure-user-authenticationauthorization)
          * [Configure email notifications](#configure-email-notifications)
      * [Deployment](#deployment)
        * [Running the DSS API locally](#running-the-dss-api-locally)
        * [Acquiring GCP credentials](#acquiring-gcp-credentials)
        * [Setting admin emails](#setting-admin-emails)
        * [Deploying the DSS](#deploying-the-dss)
          * [Resources](#resources)
          * [Buckets](#buckets)
          * [ElasticSearch](#elasticsearch)
          * [Certificates](#certificates)
          * [Deploying](#deploying)
        * [CI/CD with Travis CI and GitLab](#cicd-with-travis-ci-and-gitlab)
        * [Authorizing Travis CI to deploy](#authorizing-travis-ci-to-deploy)
        * [Authorizing the event relay](#authorizing-the-event-relay)
      * [Using the HCA Data Store CLI Client](#using-the-hca-data-store-cli-client)
      * [Checking Indexing](#checking-indexing)
      * [Running Tests](#running-tests)
      * [Development](#development)
         * [Managing dependencies](#managing-dependencies)
         * [Logging conventions](#logging-conventions)
         * [Enabling Profiling](#enabling-profiling)
      * [Contributing](#contributing)

## Getting Started

In this section, you'll configure and deploy a local API server and your own suite of cloud services to run a
development version of the DSS.

Note that privileged access to cloud accounts (AWS, GCP, etc.) is required to deploy the data-store. IF your deployment fails
due to access restrictions, please consult your local systems administrators.

### Install Dependencies

The DSS requires Python 3.6 to run.

Clone the repo and install dependencies:
```
git clone git@github.com:HumanCellAtlas/data-store.git
cd data-store
pip install -r requirements-dev.txt
```

Also install [terraform from Hashicorp](https://www.terraform.io/) from your favourite package manager.

### Configuration

The DSS is configured via environment variables. The required environment variables and their default values
are defined in the file [`environment`](environment). To customize the values of these environment variables:

1. Copy `environment.local.example` to `environment.local`
1. Edit `environment.local` to add custom entries that override the default values in `environment`
1. Run `source environment`  now and whenever these environment files are modified.

The full list of configurable environment variables and their descriptions are documented [here](docs/environment/README.md).

#### Configure Terraform

The DSS uses the [Amazon S3 backend](https://www.terraform.io/docs/backends/types/s3.html) for Terraform-deployed resources. To run deployment, make sure that the requisite bucket exists. The bucket is named after `$DSS_TERRAFORM_BACKEND_BUCKET_TEMPLATE`, by default `org-humancellatlas-{account_id}-terraform` with `{account_id}` substituted with your AWS account ID.

#### Configure AWS

1. Follow this [tutorial](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) to install the
   AWS command line utility and configure your AWS access credentials.

1. Specify the names of S3 buckets in `environment.local`, using the environment variables `DSS_S3_BUCKET_*`, and verify
   that `AWS_DEFAULT_REGION` points to your prefered region.
   These buckets will be created with Terraform, and should not exist before deploying for the first time.

#### Configure GCP

1.  Generate OAuth application secrets to be used for your instance:

    1) Go to the [GCP API and Service Credentials page](https://console.developers.google.com/apis/credentials). You may have to select Organization and Project again.

	1) Click *Create Credentials* and select *OAuth client*

	1) For *Application type* choose *Other*

	1) Under application name, use `hca-dss-` followed by the stage name (i.e. the value of `DSS_DEPLOYMENT_STAGE`). This
    is a convention only and carries no technical significance.

	1) Click *Create*, don't worry about noting the client ID and secret, click *OK*

	1) Click the edit icon for the new credentials and click *Download JSON*

	1) Place the downloaded JSON file into the project root as `application_secrets.json`

	1) Run the command

	   ```
	   cat $DSS_HOME/application_secrets.json | ./scripts/dss-ops.py secrets set --secret-name $GOOGLE_APPLICATION_SECRETS_SECRETS_NAME
       ```

       **WARNING:** Running the above command will change secrets!

1.  [Download](https://cloud.google.com/sdk/downloads) the `gcloud` command line utility.

1.  Run the command

    ```
    ./scripts/populate_lambda_ssm_parameters.py
    ```

	This populates the environment variables defining your stage into an AWS Simple Systems Manager parameter store.
    These variables will be read from the parameter store and in-lined into the Lambda deployment packages during
	deployment. This command should be executed whenever the environment variables are updated. The environments
	of currently deployed Lambdas may optionally by updated in place with the flag `--update-deployed-lambdas`.

1.  Choose a region that has support for Cloud Functions and set `GCP_DEFAULT_REGION` to that region. See
    [the GCP locations list](https://cloud.google.com/about/locations/) for a list of supported regions.

1.  Run `gcloud config set project PROJECT_ID` **where PROJECT_ID is the ID, not the name (i.e: hca-store-21555, NOT just hca-store) of the GCP project you selected earlier**.

1.  Enable required APIs:

    ```
    gcloud services enable cloudfunctions.googleapis.com
    gcloud services enable runtimeconfig.googleapis.com
    gcloud services enable iam.googleapis.com
    ```

1.  Specify the names of Google Cloud Storage buckets in `environment.local`, using the environment variables `DSS_GS_BUCKET_*`,
    and verify that `GCP_DEFAULT_REGION` points to your prefered region.
    These buckets will be created with Terraform, and should not exist before deploying for the first time.

#### Configure User Authentication/Authorization
The following environment variables must be set to enable user authentication and authorization.

* `OIDC_AUDIENCE` must be populated with the expected JWT (JSON web token) audience.
* `OPENID_PROVIDER` is the generator of the JWT, and is used to determine how the JWT is validated.
* `OIDC_GROUP_CLAIM` is the JWT claim that specifies the group the users belongs to.
* `OIDC_EMAIL_CLAIM` is the JWT claim that specifies the requests email.

Also update `authorizationUrl` in **dss/dss-api.yml** to point to an authorization endpoint which returns a
valid JWT.

Optional: To configure a custom swagger auth before deployment run:

    python scripts/swagger_auth.py -c='{"/path": "call"}'

Alternatively, to configure auth for all swagger endpoints, you can run:

    python scripts/swagger_auth.py --secure

Note: Removing auth from endpoints will currently break tests, however adding auth should be fine
(`make test` should run successfully).

Note: The auth config file for deployment can also be set in `environment.local` with AUTH_CONFIG_FILE.

#### Configure email notifications

Some daemons (dss-checkout-sfn for example) use Amazon SES to send emails. You must set `DSS_NOTIFICATION_SENDER` to
your email address and then verify that address using the SES Console enabling SES to send notification emails from it.



## Deployment

---

### Running the DSS API locally

Run `./dss-api` in the top-level `data-store` directory to deploy the DSS API on your `localhost`.

### Acquiring GCP credentials

When deploying for the first time, a Google Cloud Platform service account must first be created and credentialed.

1.  Specify the name of the Google Cloud Platform service account in `environment.local` using the variable `DSS_GCP_SERVICE_ACCOUNT_NAME`.

1.  Provision a set of credentials that will allow you to run deployment.

    1) In the [Google Cloud Console](https://console.cloud.google.com/), select the correct Google user account on the top
       right and the correct GCP project in the drop down in the top center. Go to "IAM & Admin", then "Service accounts".

    1) Click "Create service account" and select "Furnish a new private key". Under "Roles", select
       a) "Project – Owner",
       a) "Service Accounts – Service Account User"
       a) "Cloud Functions – Cloud Function Developer".

    1) Create the account and download the service account key JSON file.

    1) Place the file as `$DSS_HOME/gcp-credentials.json`. You will replace it later.

1.  Create the Google Cloud Platform service account using the command
    ```
    make -C infra COMPONENT=gcp_service_account apply
    ```
    This step can be skipped if you're rotating credentials.

1.  Place the downloaded JSON file into the project root as `gcp-credentials.json`

1.  Run the command

    ```
    cat $DSS_HOME/gcp-credentials.json | ./scripts/dss-ops.py secrets set --secret-name $GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME
    ```

### Setting admin emails

Set admin account emails within AWS Secret Manager

```
echo ' ' |  ./scripts/dss-ops.py secrets set --secret-name $ADMIN_USER_EMAILS_SECRETS_NAME
 ```

### Deploying the DSS

Assuming the tests have passed above, the next step is to manually deploy. See the section below for information on
CI/CD with Travis if continuous deployment is your goal.

Several components in the DSS deployed separately as daemons, found in `$DSS_HOME/daemons`. Daemon deployment may
incorporate dependent infrastructure, such SQS queues or SNS topics, by placing Terraform files in daemon directory, e.g.
`$DSS_HOME/daemons/dss-admin/my_queue_defs.tf`. This infrastructure is deployed non-interactively, without the
usual plan/review Terraform workflow, and should therefore be lightweight in nature. Large infrastructure should be
added to `$DSS_HOME/infra` instead.

##### Resources

Cloud resources have the potential for naming collision in both [AWS](https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)
 and [GCP](https://cloud.google.com/storage/docs/naming), ensure that you rename resources as needed.

#### Buckets

Buckets within AWS and GCP need to be available for use by the DSS. Use Terraform to setup these resources:

```
make -C infra COMPONENT=buckets plan
make -C infra COMPONENT=buckets apply
```

#### ElasticSearch

The AWS Elasticsearch Service is used for metadata indexing. Currently the DSS uses version 5.5 of ElasticSearch. For typical development deployments the
t2.small.elasticsearch instance type is sufficient. Use the [`DSS_ES_`](./docs/environment/README.md) variables to adjust the cluster as needed. 

Add allowed IPs for ElasticSearch to the secret manager, use comma separated IPs:

```
echo ' ' | ./scripts/dss-ops.py secret set --secret-name $ES_ALLOWED_SOURCE_IP_SECRETS_NAME
```

Use Terraform to deploy ES resource:

```
make -C infra COMPONENT=elasticsearch plan
make -C infra COMPONENT=elasticsearch apply
```

#### Certificates

A certificate matching your domain must be registered with
[AWS Certificate Manager](https://docs.aws.amazon.com/acm/latest/userguide/acm-overview.html). Set `ACM_CERTIFICATE_IDENTIFIER`
to the identifier of the certificate, which can be found on the AWS console.

An AWS route53 zone must be available for your domain name and configured in `environment`.

#### Deploying

Now deploy using make:

    make plan-infra
    make deploy-infra
    make deploy

If successful, you should be able to see the Swagger API documentation at:

    https://<domain_name>

And you should be able to list bundles like this:

    curl -X GET "https://<domain_name>/v1/bundles" -H  "accept: application/json"

#### Monitoring

Please see the [data-store-monitor](https://www.github.com/humancellatlas/data-store-monitor) repo for additional
monitoring tools.

### CI/CD with Travis CI and GitLab

We use [Travis CI](https://travis-ci.com/HumanCellAtlas/data-store) for continuous unit testing that does
not involve deployed components. A private [GitLab](https://about.gitlab.com) instance is used for deployment to
the `dev` environment if unit tests pass, as well as further testing of deployed components, for every commit
on the `master` branch. GitLab testing results are announced on the
`data-store-eng` Slack channel in the [HumanCellAtlas](https://humancellatlas.slack.com) workspace.
Travis behaviour is defined in `.travis.yml`, and GitLab behaviour is defined in `.gitlab-ci.yml`.

### Authorizing Travis CI to deploy

Encrypted environment variables give Travis CI the AWS credentials needed to run the tests and deploy the app. Run
`scripts/authorize_aws_deploy.sh IAM-PRINCIPAL-TYPE IAM-PRINCIPAL-NAME` (e.g. `authorize_aws_deploy.sh group
travis-ci`) to give that principal the permissions needed to deploy the app. Because a group policy has a higher size
limit (5,120 characters) than a user policy (2,048 characters), it is advisable to apply this to a group and add the
principal to that group. Because this is a limited set of permissions, it does not have write access to IAM. To set up
the IAM policies for resources in your account that the app will use, run `make deploy` using privileged account
credentials once from your workstation. After this is done, Travis CI will be able to deploy on its own. You must
repeat the `make deploy` step from a privileged account any time you change the IAM policies templates in
`iam/policy-templates/`.

### Authorizing the event relay

Environment variables provide the AWS credentials needed to relay events originating from supported cloud platforms
outside of AWS. Run `scripts/create_config_aws_event_relay_user.py` to create an AWS IAM user with the appropriate
restricted access policy. This script also creates the user access key and stores it in an AWS Secrets Manager
store.

**Note** when executing the script above, ensure that the role/user used within AWS is  authorized to perform: iam:CreateUser

## Using the HCA Data Store CLI Client

Now that you have deployed the data store, the next step is to use the HCA Data Store CLI to upload and download data to
the system. See [data-store-cli](https://github.com/HumanCellAtlas/data-store-cli) for installation instructions. The
client requires you change `hca/api_spec.json` to point to the correct host, schemes, and, possibly, basePath. Examples
of CLI use:

    # list bundles
    hca dss post-search --es-query "{}" --replica=aws | less
    # upload full bundle
    hca dss upload --replica aws --staging-bucket staging_bucket_name --src-dir ${DSS_HOME}/tests/fixtures/datafiles/example_bundle

## Checking Indexing

Now that you've uploaded data, the next step is to confirm the indexing is working properly and you can query the
indexed metadata.

    hca dss post-search --replica aws --es-query '
    {
        "query": {
            "bool": {
                "must": [{
                    "match": {
                        "files.donor_organism_json.medical_history.smoking_history": "yes"
                    }
                }, {
                    "match": {
                        "files.specimen_from_organism_json.genus_species.text": "Homo sapiens"
                    }
                }, {
                    "match": {
                        "files.specimen_from_organism_json.organ.text": "brain"
                    }
                }]
            }
        }
    }
    '

## Running Tests

1. Check that software packages required to test and deploy are available, and install them if necessary:

    `make --dry-run`

1. Populate text fixture buckets with test fixture data _**(This command will completely empty the given buckets** before populating them with test fixture data, please ensure
the correct bucket names are provided)**_:

    ```
    tests/fixtures/populate.py --s3-bucket $DSS_S3_BUCKET_TEST_FIXTURES --gs-bucket $DSS_GS_BUCKET_TEST_FIXTURES
    ```

1. Set the environment variable `DSS_TEST_ES_PATH` to the path of the `elasticsearch` binary on your machine.

1. Run tests with `make test`

### Test suites

All tests for the DSS fall into one of two categories:

* *Standalone tests*, which do not depend on deployed components, and
* *Integration tests*, which depend on deployed components.

As such, standalone tests can be expected to pass even if no deployment is configured,
and in fact should pass before an initial deployment. For more information on tests,
see [tests/README.md](tests/README.md).

## Development

### Managing dependencies

The direct runtime dependencies of this project are defined in `requirements.txt.in`. Direct development dependencies
are defined in `requirements-dev.txt.in`. All dependencies, direct and transitive, are defined in the corresponding
`requirements.txt` and `requirements-dev.txt` files. The latter two can be generated using `make requirements.txt` or
`make requirements-dev.txt` respectively. Modifications to any of these four files need to be committed. This process is
aimed at making dependency handling more deterministic without accumulating the upgrade debt that would be incurred by
simply pinning all direct and transitive dependencies.  Avoid being overly restrictive when constraining the allowed
version range of direct dependencies in -`requirements.txt.in` and `requirements-dev.txt.in`

If you need to modify or add a direct runtime dependency declaration, follow the steps below:

1) Make sure there are no pending changes to `requirements.txt` or `requirements-dev.txt`.
1) Make the desired change to `requirements.txt.in` or `requirements-dev.txt.in`
1) Run `make requirements.txt`.  Run `make requirements-dev.txt` if you have modified `requirements-dev.txt.in`.
1) Visually check the changes to `requirements.txt` and `requirements-dev.txt`.
1) Commit them with a message like `Bumping dependencies`.

You now have two commits, one that catches up with updates to transitive dependencies, and one that tracks your explict
change to a direct dependency. This process applies to development dependencies as well, except for
`requirements-dev.txt` and `requirements-dev.txt.in` respectively.

If you wish to re-pin all the dependencies, run `make refresh_all_requirements`.  It is advisable to do a full
test-deploy-test cycle after this (the test after the deploy is required to test the lambdas).

### Logging conventions

1.  Always use a module-level logger, call it `logger` and initialize it as follows:

    ```python
    import logging
    logger = logging.getLogger(__name__)
    ```

1.  Do not configure logging at module scope. It should be possible to import any module without side-effects on
    logging. The `dss.logging` module contains functions that configure logging for this application, its Lambda
    functions and unit tests.

1.  When logging a message, pass either

    * an f-string as the first and only positional argument or

    * a %-string as the first argument and substitution values as subsequent arguments. Do not mix the two string
      interpolation methods. If you mix them, any percent sign in a substituted value will raise an exception.

    ```python
    # In other words, use
    logger.info(f"Foo is {foo} and bar is {bar}")
    # or
    logger.info("Foo is %s and bar is %s", foo, bar)
    # but not
    logger.info(f"Foo is {foo} and bar is %s", bar)
    # Keyword arguments can be used safely in conjunction with f-strings:
    logger.info(f"Foo is {foo}", exc_info=True)
    ```

1.  To enable verbose logging by application code, set the environment variable `DSS_DEBUG` to `1`. To enable verbose
    logging by dependencies set `DSS_DEBUG` to `2`. To disable verbose logging unset `DSS_DEBUG` or set it to `0`.

1.  To assert in tests that certain messages were logged, use the `dss` logger or one of its children

    ```python
    dss_logger = logging.getLogger('dss')
    with self.assertLogs(dss_logger) as log_monitor:
        # do stuff
    # or
    import dss
    with self.assertLogs(dss.logger) as log_monitor:
        # do stuff
    ```

### Enabling Profiling

AWS Xray tracing is used for profiling the performance of deployed lambdas. This can be enabled for `chalice/app.py` by
setting the lambda environment variable `DSS_XRAY_TRACE=1`. For all other daemons you must also check
"Enable active tracking" under "Debugging and error handling" in the AWS Lambda console.

## Security Policy

See our [Security Policy](https://github.com/HumanCellAtlas/.github/blob/master/SECURITY.md).

## Contributing

External contributions are welcome. Please review the [Contributing Guidelines](CONTRIBUTING.md)

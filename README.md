# DSS: The Data Storage System

[![Build Status](https://travis-ci.com/DataBiosphere/data-store.svg?branch=master)](https://travis-ci.com/DataBiosphere/data-store)
[![codecov](https://codecov.io/gh/DataBiosphere/data-store/branch/master/graph/badge.svg)](https://codecov.io/gh/DataBiosphere/data-store)
<a width="120" height="20" href="https://auth0.com/?utm_source=oss&utm_medium=gp&utm_campaign=oss" target="_blank" alt="Single Sign On & Token Based Authentication - Auth0"><img width="150" height="50" alt="JWT Auth for open source projects" src="https://cdn.auth0.com/oss/badges/a0-badge-light.png"/></a>

This repository maintains the data storage system. We use this
[Google Drive folder](https://drive.google.com/open?id=0B-_4IWxXwazQbWE5YmtqUWx3RVE) for design docs and
meeting notes, and [this Zenhub board](https://app.zenhub.com/workspace/o/databiosphere/data-store) to track our GitHub work.

## Overview

The DSS is a replicated data storage system designed for hosting large sets of scientific experimental data on
[Amazon S3](https://aws.amazon.com/s3/) and [Google Storage](https://cloud.google.com/storage/). The DSS exposes an API
for interacting with the data and is built using [Chalice](https://github.com/aws/chalice),
[API Gateway](https://aws.amazon.com/api-gateway/) and [AWS Lambda](https://aws.amazon.com/lambda/). The API also
implements [Step Functions](https://aws.amazon.com/step-functions/) to orchestrate Lambdas for long-running tasks such
as large file writes. You can find the API documentation and give it a try [here](https://dss.data.ucsc-cgp-redwood.org/).

### Architectural Diagram

[![DSS Sync SFN diagram](https://www.lucidchart.com/publicSegments/view/43dfe33a-47c9-466b-9cb6-6d941a406d8f/image.png)](https://www.lucidchart.com/documents/view/b65c8898-46e3-4560-b3b2-9e85f1c0a4c7)

### DSS API

The DSS API uses [Swagger](http://swagger.io/) to define the [API specification](dss-api.yml) according to the
[OpenAPI 2.0 specification](https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md).
[Connexion](https://github.com/zalando/connexion) is used to map the API specification to its implementation in Python.

You can use the
[Swagger Editor](http://editor.swagger.io/#/?import=https://raw.githubusercontent.com/DataBiosphere/data-store/master/dss-api.yml)
to review and edit the API specification. When the API is live, the spec is also available at `/v1/swagger.json`.

The DSS API Swagger is also available at <https://dss.dev.ucsc-cgp-redwood.org>.

## Table of Contents

* [DSS: The Data Storage System](#dss-the-data-storage-system)
  * [Overview](#overview)
    * [Architectural Diagram](#architectural-diagram)
    * [DSS API](#dss-api)
  * [Table of Contents](#table-of-contents)
  * [Getting Started](#getting-started)
    * [Install Dependencies](#install-dependencies)
      * [Python Dependencies](#python-dependencies)
      * [AWS and GCP CLI Tools](#aws-and-gcp-cli-tools)
      * [Terraform](#terraform)
      * [Other Utilities](#other-utilities)
    * [Configuration](#configuration)
      * [Configure Data Store](#configure-data-store)
      * [Configure Terraform](#configure-terraform)
      * [Configure AWS](#configure-aws)
      * [Configure GCP](#configure-gcp)
      * [Configure User Authentication/Authorization](#configure-user-authenticationauthorization)
      * [Configure Email Notifications](#configure-email-notifications)
  * [Deployment](#deployment)
    * [Running the DSS API locally](#running-the-dss-api-locally)
    * [Acquiring GCP Credentials](#acquiring-gcp-credentials)
    * [Setting Admin Emails](#setting-admin-emails)
    * [Deploying the DSS](#deploying-the-dss)
      * [Naming Resources](#naming-resources)
      * [Deploying Buckets](#deploying-buckets)
      * [Deploying ElasticSearch](#deploying-elasticsearch)
      * [Setting the Elasticsearch Endpoint](#setting-the-elasticsearch-endpoint)
      * [Updating the Lambda Environment](#updating-the-lambda-environment)
      * [Checking the Lambda Environment](#checking-the-lambda-environment)
      * [Domains and Certificates](#domains-and-certificates)
      * [Creating AWS Event Relay User](#creating-aws-event-relay-user)
      * [Deploying](#deploying)
      * [Monitoring](#monitoring)
      * [Updating Environment Variables](#updating-environment-variables)
      * [Existing Infrastructure](#existing-infrastructure)
    * [CI/CD with Travis CI and GitLab](#cicd-with-travis-ci-and-gitlab)
    * [Authorizing Travis CI to deploy](#authorizing-travis-ci-to-deploy)
    * [Authorizing the event relay](#authorizing-the-event-relay)
  * [Using the Data Store CLI Client](#using-the-data-store-cli-client)
  * [Checking Indexing](#checking-indexing)
  * [Running Tests](#running-tests)
    * [Test suites](#test-suites)
  * [Development](#development)
    * [Managing dependencies](#managing-dependencies)
    * [Logging conventions](#logging-conventions)
    * [Enabling Profiling](#enabling-profiling)
  * [Security Policy](#security-policy)
  * [Contributing](#contributing)

## Getting Started

In this section, you'll configure and deploy a development version of the DSS, consisting of a local API server and
a suite of cloud services.

All commands given in this Readme should be run from the root of this repository after sourcing the
correct environment (see the [Configuration](#configuration) section below). The root directory of the repository
is also available in the environment variable `$DSS_HOME`.

**NOTE:** Deploying the data store requires privileged access to cloud accounts (AWS, GCP, etc.).
If your deployment fails due to access restrictions, please consult your local system administrators.

The first step to get started with the data store is to clone this repository:

```
git clone git@github.com:DataBiosphere/data-store.git
cd data-store
```

### Install Dependencies

#### Python Dependencies

The DSS requires Python 3.6+ to run. The file `requirements.txt` contains Python dependencies for those running a data store,
and `requirements-dev.txt` contains Python dependencies for those developing code for the data store. Once this
repository has been cloned, use pip to install the Python dependencies:

```
pip install -r requirements-dev.txt
```

#### AWS and GCP CLI Tools

To interact with AWS and GCP from the command line, use the officially distributed CLI tools.

The `aws` CLI tool can be installed via `pip install awscli` (or any other method covered in the
[aws-cli repository Readme](https://github.com/aws/aws-cli#installation)).

The `gcloud` CLI tool should be installed directly from Google Cloud. Use the [`gcloud`
Downloads](https://cloud.google.com/sdk/downloads) page to download the latest version.  Use the [`gcloud`
Quickstarts](https://cloud.google.com/sdk/docs/quickstarts/) page for installation instructions for various
operating systems.

#### Terraform

[Terraform](https://www.terraform.io), a tool From Hasicorp, should also be [downloaded from
terraform.io](https://www.terraform.io/downloads.html) and the binary moved somewhere on your `$PATH`.

The data store requires that a specific version of Terraform be used. Check [`common.mk`](common.mk) for the
specific version of Terraform that should be installed.

**NOTE:** The Dockerfile for the CI/CD test cluster, [`allspark.Dockerfile`](allspark.Dockerfile), contains
a set of commands to download and install a specified version of Terraform.

#### Other Utilities

The data store makes use of a number of other command line utilities that should be present on your system (if they
are not, `make` commands will fail):

* `jq` - install via `apt-get install jq` or `brew install jq`
* `sponge` - install via `apt-get install moreutils` or `brew install moreutils`
* `envsubst` - install via `apt-get install gettext` or `brew install gettext && brew link gettext`

See the file `common.mk` for more information.

### Configuration

#### Configure Data Store

The DSS is configured via environment variables.

The file [`environment`](environment) sets default values for all variables used in the data store.  The file
[`environment.local`](environment.local) overrides default values with custom entries. To customize the
configuration environment variables:

1. Copy `environment.local.example` to `environment.local`
1. Edit `environment.local` to add custom entries that override the default values in `environment`
1. Run `source environment`  now and whenever these environment files are modified.

When the user runs `source environment`, it will execute the entire `environment` file, setting each variable to its
default value; then `environment` will source `environment.local`, overwriting the default values with the new
values defined in `environment.local`.

The full list of configurable environment variables and their descriptions is [here](docs/environment/README.md).

#### Configure Terraform

The DSS uses Terraform's [AWS S3 backend](https://www.terraform.io/docs/backends/types/s3.html) for deployment.
This means Terraform will use an AWS S3 bucket to store its configuration files.

Before Terraform is used, the Terraform bucket that will contain the configuration files must be created -
Terraform will not create this bucket itself. Specify the bucket name using the environment variable
`$DSS_TERRAFORM_BACKEND_BUCKET_TEMPLATE`.

All other buckets will be created by Terraform during the infrastructure deployment step and should not exist
before deploying for the first time.

#### Configure AWS

To configure the AWS CLI:

1. Configure your AWS CLI credentials following the data store [AWS CLI Configuration Guide](docs/aws_cli_config.md).

1. Verify that `AWS_DEFAULT_REGION` points to your prefered AWS region.

1. Specify the names of S3 buckets in `environment.local` using the environment variables `DSS_S3_BUCKET_*`.
    These buckets will be created by Terraform and should not exist before deploying.

#### Configure GCP

To configure GCP for deployment of infrastructure, start by creating an OAuth application and generating associated
tokens. These will be stored in the AWS Secrets Manager and used for automated deployment of infrastructure to
GCP. Here are the steps:

1. Go to the [GCP API and Service Credentials page](https://console.developers.google.com/apis/credentials). You
   may have to select Organization and Project again.

1. Click *Create Credentials* and select *OAuth client*

1. For *Application type* choose *Other*

1. Under application name, use `${DSS_PLATFORM}-dss-` followed by the stage name (i.e. the value of `DSS_DEPLOYMENT_STAGE`.. This
is a convention only and carries no technical significance.

1. Click *Create*, don't worry about noting the client ID and secret, click *OK*

1. Click the edit icon for the new credentials and click *Download JSON*

1. Place the downloaded JSON file into the project root as `application_secrets.json`

1. Run the following command to store `application_secrets.json` in the AWS Secrets Manager
   (to make it available later during the deployment process)

   ```
   ### WARNING: RUNNING THIS COMMAND WILL
   ###          CLEAR EXISTING SCRET VALUE
   cat $DSS_HOME/application_secrets.json | ./scripts/dss-ops.py secrets set --force $GOOGLE_APPLICATION_SECRETS_SECRETS_NAME
   ```

Next, configure the `gcloud` command line utility with the following steps:

1.  Choose a region that has support for Cloud Functions and set `GCP_DEFAULT_REGION` to that region. See
    [the GCP locations list](https://cloud.google.com/about/locations/) for a list of supported regions.

1.  Run `gcloud config set project PROJECT_ID`, where `PROJECT_ID` is the ID of the project, not the name (i.e:
    `dss-store-21555`, NOT just `dss-store`) of the GCP project you selected earlier.

1. Enable the required APIs:

    ```
    gcloud services enable cloudfunctions.googleapis.com
    gcloud services enable runtimeconfig.googleapis.com
    gcloud services enable iam.googleapis.com
    ```

1.  Specify the names of Google Cloud Storage buckets in `environment.local` using the environment variables `DSS_GS_BUCKET_*`.
    These buckets will be created by Terraform and should not exist before deploying.

#### Configure User Authentication/Authorization

The following environment variables must be set to enable user authentication and authorization:

* `OIDC_AUDIENCE` must be populated with the expected JWT (JSON web token) audience.
* `OPENID_PROVIDER` is the generator of the JWT, and is used to determine how the JWT is validated.
* `OIDC_GROUP_CLAIM` is the JWT claim that specifies the group the users belongs to.
* `OIDC_EMAIL_CLAIM` is the JWT claim that specifies the requests email.

Also update `authorizationUrl` in `dss-api.yml` to point to an authorization endpoint that will return
a valid JWT.

Optional: To configure a custom swagger auth before deployment run:

    python scripts/swagger_auth.py -c='{"/path": "call"}'

Alternatively, to configure auth for all swagger endpoints, you can run:

    python scripts/swagger_auth.py --secure

Note: Removing auth from endpoints will currently break tests, however adding auth should be fine
(`make test` should run successfully).

Note: The auth config file for deployment can also be set in `environment.local` with `AUTH_CONFIG_FILE`.

#### Configure Email Notifications

Some daemons (`dss-checkout-sfn` for example) use Amazon SES to send emails. You must set `DSS_NOTIFICATION_SENDER`
to your email address, then verify that email address using the SES Console. This will enable SES to send notification
emails.

## Deployment

### Running the DSS API locally

Run `./dss-api` in the top-level `data-store` directory to deploy the DSS API on your `localhost`.

### Acquiring GCP Credentials

We use Terraform to automatically create a Google Cloud service account (referred to as the "deployment service account")
to deploy Google Cloud infrastructure.

When deploying for the first time, we need to manually create a (different) service account (referred to as the
"utility service account") that Terraform can utilize to create the deployment service account. The utility service
account is only used once, during the first deployment, to create the deployment service account.

To manually create the utility service account:

1.  In the [Google Cloud Console](https://console.cloud.google.com/), select the correct Google user account on the top
    right and the correct GCP project in the drop down in the top center. Go to "IAM & Admin", then "Service accounts".

1.  Click "Create service account" and select "Furnish a new private key". Under "Roles", select the following
    roles:

    a) "Project – Owner"

    b) "Service Accounts – Service Account User"

    c) "Cloud Functions – Cloud Function Developer"

1.  Create the account and download the utility service account key JSON file.

1.  Place the file at `$DSS_HOME/gcp-credentials-util.json`. Terraform will use this utility service account credentials
    file to create the deployment service account.

Now that we have the utility service account credentials, we can use Terraform to create the deployment service
account:

1.  Specify the name of the Google Cloud Platform deployment service account in `environment.local` using the environment
    variable `DSS_GCP_SERVICE_ACCOUNT_NAME`. It should be set to `$DSS_HOME/gcp-credentials-util.json`.

1.  Specify that you want to use the utility service account credentials to create the deployment service account by
    setting `GOOGLE_APPLICATION_CREDENTIALS` to `$DSS_HOME/gcp-credentials-util.json`:

    ```
    export GOOGLE_APPLICATION_CREDENTIALS="$DSS_HOME/gcp-credentials-util.json"
    ```

1.  Create the Google Cloud Platform deployment service account using the command

    ```
    make -C infra COMPONENT=gcp_service_account apply
    ```

    Alternatively, an existing service account can be imported instead using `terraform import` from the Google
    service account component directory:

    ```
    cd infra/gcp_service_account
    terraform import google_service_account.dss ${DSS_GCP_SERVICE_ACCOUNT_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com
    ```

    This step can be skipped if you're rotating credentials.

1.  Once the deployment service account has been created, open the Google Cloud Platform web console and navigate
    to "IAM & Admin", then "Service accounts". Click the menu on the right and select the "Create new key" option.
    Create and download a new JSON key and place the downloaded key into the project root at
    `${DSS_HOME}/gcp-credentials.json`.

1.  Store the deployment service account credentials just downloaded in the AWS Secrets Manager:

    ```
    ### WARNING: RUNNING THIS COMMAND WILL
    ###          CLEAR EXISTING SECRET VALUE
    cat $DSS_HOME/gcp-credentials.json | ./scripts/dss-ops.py secrets set --force $GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME
    ```

Lastly, when you have finished creating the deployment service account, switch to its credentials by resetting
`GOOGLE_APPLICATION_CREDENTIALS` to the deployment service account credentials file, which should be at
`${DSS_HOME}/gcp-credentials.json`:

    GOOGLE_APPLICATION_CREDENTIALS=${DSS_HOME}/gcp-credentials.json

Note that if you are having problems with GCP credentials that look like this:

    Error applying IAM policy for project "${GCP_PROJECT_ID}":
    Error setting IAM policy for project "${GCP_PROJECT_ID}":
    googleapi: Error 403: The caller does not have permission, forbidden

double-check that your `GOOGLE_APPLICATION_CREDENTIALS` are set to the utility
service account, and not the deployment service account - otherwise the deployment
service account is trying to modify itself!

### Setting Admin Emails

Set admin account emails within AWS Secret Manager:

    ### WARNING: RUNNING THIS COMMAND WILL
    ###          CLEAR EXISTING SECRET VALUE
    echo -n 'user1@example.com,user2@example.com' |  ./scripts/dss-ops.py secrets set --force $ADMIN_USER_EMAILS_SECRETS_NAME

Alternatively, define `ADMIN_USER_EMAILS` in `environment.local` and run:

    ### WARNING: RUNNING THIS COMMAND WILL
    ###          CLEAR EXISTING SECRET VALUE
    echo -n $ADMIN_USER_EMAILS |  ./scripts/dss-ops.py secrets set --force $ADMIN_USER_EMAILS_SECRETS_NAME

### Deploying the DSS

Assuming the tests have passed above, the next step is to manually deploy. See the section below for information on
CI/CD with Travis if continuous deployment is your goal.

Several components in the DSS are deployed separately as daemons, found in `$DSS_HOME/daemons`. Daemon deployment may
be dependent on infrastructure being deployed, such SQS queues or SNS topics. This infrastructure can be handled by placing
Terraform files in the daemon directory, e.g., `${DSS_HOME}/daemons/dss-admin/my_queue_defs.tf`. This infrastructure is
deployed non-interactively, without the usual Terraform workflow of planning and reviewing. Therefore it should be
lightweight in nature.

More complex or larger infrastructure should be added to `$DSS_HOME/infra` instead of the daemon infrastructure
whenever possible.

#### Naming Resources

Both [AWS](https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) and
[GCP](https://cloud.google.com/storage/docs/naming) use global namespaces shared amongst all
users, so ensure that you name your resources appropriately to avoid name collisions.

#### Deploying Buckets

Buckets within AWS and GCP need to be available for use by the DSS. Use Terraform to set up the buckets:

```
make -C infra COMPONENT=buckets plan
make -C infra COMPONENT=buckets apply
```

#### Deploying ElasticSearch

The AWS Elasticsearch Service is used for metadata indexing. Currently the DSS uses version 5.5 of ElasticSearch. For typical development deployments the
t2.small.elasticsearch instance type is sufficient. Use the [`DSS_ES_`](./docs/environment/README.md) variables to adjust the cluster as needed.

The operator doing the deployment must add their public IP address as an allowed IP address to access the Elasticsearch cluster.
Allowed Elasticsearch IP addresses should be added to the secrets manager; separate IP addresses with commas. For
example, if the public IP addresses of two operators needing to deploy a data store are `1.1.1.1` and `2.2.2.2`,
the Elasticsearch allowed source IPs variable would be set like so:

```
### WARNING: RUNNING THIS COMMAND WILL
###          CLEAR EXISTING SECRET VALUE
echo -n '1.1.1.1,2.2.2.2' | ./scripts/dss-ops.py secrets set --force $ES_ALLOWED_SOURCE_IP_SECRETS_NAME
```

Use Terraform to deploy ES resource:

```
make -C infra COMPONENT=elasticsearch plan
make -C infra COMPONENT=elasticsearch apply
```

#### Setting the Elasticsearch Endpoint

Open the AWS Web Console and navigate to the Elasticsearch Service.
The Elasticsearch domain with the name matching `DSS_ES_DOMAIN` should
show up in the list. Open this Elasticsearch domain. The Elasticsearch
endpoint will be shown there, and will look something like:

```
https://search-${DSS_ES_DOMAIN}-abcxyz1234567890.${AWS_REGION}.es.amazonaws.com
```

Now set the environment variable `DSS_ES_ENDPOINT` in `environment.local` to this
Elasticsearch url, minus the `https://` prefix. For example,

```
DSS_ES_ENDPOINT="search-${DSS_ES_DOMAIN}-abcxyz1234567890.${AWS_REGION}.es.amazonaws.com "
```

Note that it should **not** be stored in a version-controlled file like `environment`, but should be stored in
the local environment file `environment.local` instead. Export the new environment variable values with
`source environment` once the new variable is set.

#### Updating the Lambda Environment

Once the `DSS_ES_ENDPOINT`, `DSS_ES_ALLOWED_IPS`, and `ADMIN_USER_EMAILS` environment variables have been set,
all variables required by the lambda functions have been set, so the next step is to export the lambda function
environment variables in the local environment and store it in the parameter store under the variable
`environment`. These environment variables will then be set in each lambda function during the deployment step.

To export the lambda function environment variables, use the `lambda update` function of the dss operations script:

```
./scripts/dss-ops.py lambda update
```

If there are already lambda functions deployed, you can add the `--update-deployed` flag to export the variables to
all deployed lambda functions, in addition to exporting the variables to the parameter store.

```
./scripts/dss-ops.py lambda update --update-deployed
```

#### Checking the Lambda Environment

It is useful to be able to check on the lambda environments to troubleshoot problems with a data store
deployment. There are two ways to check the lambda environment, both using the `./scripts/dss-ops.py` script:

1. Print lambda environment variables and values from the currently-deployed lambdas. These are the environment
   variable values that are **currently** deployed to the lambdas (and therefore may not match values in the
   parameter store or in your local environment).

   ```
   ./scripts/dss-ops.py lambda environment
   ```

1. Print lambda environment variables and values stored in the parameter store. These are the environment variable
   values that **will be** deployed to the lambdas during the next deployment.

   ```
   ./scripts/dss-ops.py params environment
   ```

#### Domains and Certificates

It is assumed that [Route 53](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html) and the
[AWS Certificate Manager](https://docs.aws.amazon.com/acm/latest/userguide/acm-overview.html) are used to
manage domains and HTTPS certificates for those domains.

The first step is to verify the domain that the data store will use should be listed as a Hosted Zone in Route 53.
To verify, open the AWS Web Console, select *Route 53*, then select *Hosted Zones*.

The next step is to create a wildcard certificate for your domain. Your ownership or control of the domain must be
verified to create a certificate matching the domain. We recommended to use the DNS method of verification, as
this is well-integrated with Route 53.

1. Open the AWS Web Console and select the AWS Certificate Manager.
1. Click "Request a Certificate".
1. Select "Request a public certificate" and click "Next".
1. Enter the domain or subdomain you want the data store to use. You can use `*.example.com` to create
   a wildcard cert for an entire domain, or `*.data.example.com` to create a wildcard cert for the
   `data.example.com` subdomain only.
1. Select "DNS validation" as the domain validation method.
1. Optionally, add relevant tags (Name, Owner, Project, etc.) and click "Review".
1. Click "Confirm and Request". This will inform you that the cert is pending validation and requires you to
   verify ownership.
1. Click the triangle next to the domain name to expand the cert request. Click "Create a record in Route 53".
1. The Certificate Manager will ask you to confirm creation of a Route 53 DNS record. Click "Create", then
   "Continue".
1. Wait for the validation step to complete. Once the certificate validation step has finished, the "Status" will
   change to "Issued".

Once you have created your certificate, set `ACM_CERTIFICATE_IDENTIFIER` to the identifier of the certificate,
which can be found on the AWS console.

Note that if you are having problems with certificates that look like this:

    aws_api_gateway_domain_name.dss: Creating...
    Error: Error creating API Gateway Domain Name: BadRequestException: The provided certificate does not exist.

double-check that the certificate you have created in Certificate Manager was created in the same region specified
in your `environment` file by the variable `AWS_DEFAULT_REGION`.

#### Creating AWS Event Relay User

One last piece of infrastructure that must be created before deployment is the AWS Event Relay User.  The event
relay ([`daemons/dss-gs-event-relay`](daemons/gss-gs-event-relay)) is responsible for transmitting events from AWS
to GCP. Running this script will create a user, which requirest the `iam:CreateUser` privilege, which is granted to
project admins on the GCP account.

```
# This script must be run by a GCP project admin
./scripts/create_config_aws_event_relay_user.py
```

If you do not run this step, the `make deploy` command will fail due to a missing secret in the secrets manager.

#### Deploying

Now deploy using make:

    make plan-infra
    make deploy-infra
    make deploy

If successful, you should be able to see the Swagger API documentation at:

    https://${API_DOMAIN_NAME}

And you should be able to list bundles like this:

    curl -X GET "https://${API_DOMAIN_NAME}/v1/bundles" -H  "accept: application/json"

#### Monitoring

Please see the [data-store-monitor](https://www.github.com/humancellatlas/data-store-monitor) repo for additional
monitoring tools.

#### Updating Environment Variables

Updating environment variables defined in either `environment` or `environment.local` requires
some care when generating Terraform files from those environment variables.

Many environment variables make their way into Terraform files via templates that are made with
make commands, so when you change your `environment` or `environment.local` file, it is not enough
to just run `source environment`.

The Terraform files that store variable values are called `variables.tf` and are located in
subdirectories of the `infra/` folder. These `variable.tf` files are Terraform files automatically
generated by the `infra/build_deploy_config.py` script. To remake these files, re-create all of the
`variables.tf` files in `infra/` by running the make command:

```
# Remake all variables.tf files in infra/
make plan-infra
```

To remake `variables.tf` for a particular component,

```
# Remake variables.tf files for bucket infra
make -C infra COMPONENT=buckets plan
```

Note that `make` commands should always be used, otherwise the operator may experience problems during
the deployment process.

#### Existing Infrastructure

What happens when the deployment process tries to create resources, but those resources already exist?

Here, we have two options:

1. Import an existing resource, so that Terraform can manage and use it as part of the
   data store's infrastructure

1. Delete and re-create infrastructure

**Importing Existing Resources:**

The `terraform import` command allows Terraform to import infrastructure so that it can be
managed as part of the data store's infrastructure. This command must be run directly, there
are no `make` commands for it.

Here is an example of how to import an existing Google service account:

```
cd infra/gcp_service_account
terraform import google_service_account.dss ${DSS_GCP_SERVICE_ACCOUNT_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com
```

and another example to import an existing DynamoDB table for the async step function database:

```
cd infra/async_state_db/
terraform import aws_dynamodb_table.sfn_state dss-async-state-dev
```

The first argument after `terraform import` will be the resource identifier, and the second argument
is the name of the resource that you would like to import.

**Deleting and Re-Creating Resources:**

You can use the Makefile to destroy infrastructure resources, just as you can use it
to create them.

For example, to delete all buckets (note: buckets must be empty! If they are not,
remove their contents with the `aws s3` command), use the following make command:

```
# WARNING: THIS WILL DELETE ALL DATA STORE BUCKETS!
make -C infra COMPONENT=buckets destroy
```

If you wish to destroy **all** infra resources, use the following make command:

```
# WARNING: THIS WILL DELETE ALL DATA STORE INFRASTRUCTURE!
make -C infra destroy-all
```

Note that the infrastructure being destroyed uses names from the `environment` file,
so if the `environment` file variables do not match existing infrastructure, the
`make -C infra destroy-all` command will not work. You may also need to re-make the
Terraform `variables.tf` files, as covered in the prior section.

Once the `make destroy-all` command has been used to destroy existing infrastructure,
you can use the same `make deploy-infra` command covered above to re-create all the infrastructure.

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

## Using the Data Store CLI Client

Now that you have deployed the data store, the next step is to use the Data Store CLI client `dbio` to upload and
download data to the system. See the [data-store-cli](https://github.com/DataBiosphere/data-store-cli) repo for 
installation instructions. 

Examples of CLI use:

    # list bundles
    dbio dss post-search --es-query "{}" --replica=aws | less

    # upload full bundle
    dbio dss upload --replica aws --staging-bucket staging_bucket_name --src-dir ${DSS_HOME}/tests/fixtures/datafiles/example_bundle

## Checking Indexing

Now that you've uploaded data, the next step is to confirm the indexing is working properly and you can query the
indexed metadata.

    dbio dss post-search --replica aws --es-query '
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

1. Populate text fixture buckets with test fixture data **(This command will completely empty the given buckets
   before populating them with test fixture data, please ensure the correct bucket names are provided)**:

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


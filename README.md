# HCA DSS: The Human Cell Atlas Data Storage System

This repository contains design specs and prototypes for the
replicated data storage system (aka the "blue box") of
the [Human Cell Atlas](https://www.humancellatlas.org/).

See the [google drive folder](https://drive.google.com/open?id=0B-_4IWxXwazQbWE5YmtqUWx3RVE) for live collaborative documents.

#### About this prototype
The prototype in this repository uses [Swagger](http://swagger.io/) to specify the API in [dss-api.yml](dss-api.yml), and
[Connexion](https://github.com/zalando/connexion) to map the API specification to its implementation in Python.

You can use the
[Swagger Editor](http://editor.swagger.io/#/?import=https://raw.githubusercontent.com/HumanCellAtlas/data-store/master/dss-api.yml)
to review and edit the prototype API specification. When the prototype app is running, the Swagger spec is also available at
`/v1/swagger.json`.

The prototype is deployed continuously from the `master` branch, with the resulting producer and consumer API available at
https://hca-dss.czi.technology/.

#### Installing dependencies for development on the prototype
The HCA DSS prototype development environment requires Python 3.4+ to run. Run `pip install -r requirements-dev.txt` in this directory.

#### Installing dependencies for the prototype
The HCA DSS prototype requires Python 3.4+ to run. Run `pip install -r requirements.txt` in this directory.

#### Pull sample data bundles

Tests also use data from the data-bundle-examples subrepository.
Run: `git submodule update --init`

#### Configuring cloud-specific access credentials

**AWS**: Follow the instructions in
http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html to get the `aws` command line
utility. Create an S3 bucket that you want DSS to use. Set the environment variable `DSS_S3_TEST_BUCKET`. If you wish to
run the unit tests, you must create a second S3 bucket to store the test fixtures, and set the environment variable
`DSS_S3_TEST_SRC_DATA_BUCKET` to the name of that bucket.

**GCP**: Follow the instructions in https://cloud.google.com/sdk/downloads to get the `gcloud` command line utility.
Next, go to https://console.cloud.google.com/. Select the correct Google user account on the top right and the correct
GCP project in the drop down in the top center. Go to "IAM & Admin", then "Service accounts", then click "Create service
account" and select "Furnish a new private key". Create the account and download the service account key JSON file. Run
`gcloud auth activate-service-account --key-file=/path/to/service-account.json`. Run `gcloud config set project 'PROJECT
NAME'`. Create a bucket on Google Cloud Platform and set the environment variable `DSS_GCS_TEST_BUCKET`.  If you wish to
run the unit tests, you must create a second Google Cloud Platform bucket to store the test fixtures, and set the
environment variable `DSS_GS_TEST_SRC_DATA_BUCKET` to the name of that bucket.

**Azure**: Set the environment variables `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`.

#### Running the prototype
Run `./dss-api` in this directory.

#### Running tests
Run `make test` in this directory.

Some tests require the Elasticsearch service to be running on the local system.
Run: `elasticsearch`

#### CI/CD with Travis CI
We use [Travis CI](https://travis-ci.org/HumanCellAtlas/data-store) for continuous integration testing and
deployment. When `make test` succeeds, Travis CI deploys the application into the `dev` stage on AWS for every commit
that goes on the master branch. This behavior is defined in the `deploy` section of `.travis.yml`.

#### Authorizing Travis CI to deploy
Encrypted environment variables give Travis CI the AWS credentials needed to run the tests and deploy the app. Run
`scripts/authorize_aws_deploy.sh IAM-PRINCIPAL-TYPE IAM-PRINCIPAL-NAME` (e.g. `authorize_aws_deploy.sh user hca-test`)
to give that principal the permissions needed to deploy the app. Because this is a limited set of permissions, it does
not have write access to IAM. To set up the IAM policies for resources in your account that the app will use, run `make
deploy` using privileged account credentials once from your workstation. After this is done, Travis CI will be able to
deploy on its own. You must repeat the `make deploy` step from a privileged account any time you change the IAM policies
in `policy.json.template` files.

[![](https://img.shields.io/badge/slack-%23data--store-557EBF.svg)](https://humancellatlas.slack.com/messages/data-store/)
[![Build Status](https://travis-ci.org/HumanCellAtlas/data-store.svg?branch=master)](https://travis-ci.org/HumanCellAtlas/data-store)

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

#### Running the prototype
Run `./dss-api` in this directory.

#### Running tests
Run `make test` in this directory.

Note: Some tests require the Elasticsearch service to be running on the local system.
Run: elasticsearch

#### Configuring cloud-specific access credentials

**AWS**: Follow the instructions in
http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html. Create an S3 bucket that you want DSS to
use. Set the environment variable `DSS_S3_TEST_BUCKET`.

**GCE**: Go to https://console.cloud.google.com/. Select the correct Google user account on the top right and the
correct GCE project in the drop down in the top center. Go to "IAM & Admin", then "Service accounts", then click "Create
service account" and select "Furnish a new private key". Create the account and download the service account key JSON
file. Run `gcloud auth activate-service-account --key-file=/path/to/service-account.json`. Run `gcloud config set
project 'PROJECT NAME'`. Set the environment variable `DSS_GCS_TEST_BUCKET`.

**Azure**: Set the environment variables `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`.

[![](https://img.shields.io/badge/slack-%23data--store-557EBF.svg)](https://humancellatlas.slack.com/messages/data-store/)
[![Build Status](https://travis-ci.org/HumanCellAtlas/data-store.svg?branch=master)](https://travis-ci.org/HumanCellAtlas/data-store)

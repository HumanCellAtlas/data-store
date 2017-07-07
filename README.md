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
The HCA DSS prototype development environment requires Python 3.6+ to run. Run `pip install -r requirements-dev.txt` in this directory.

#### Installing dependencies for the prototype
The HCA DSS prototype requires Python 3.6+ to run. Run `pip install -r requirements.txt` in this directory.

#### Pull sample data bundles

Tests also use data from the data-bundle-examples subrepository.
Run: `git submodule update --init`

#### Configuring cloud-specific access credentials

**AWS**: Follow the instructions in
http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html to get the `aws` command line
utility. Create an S3 bucket that you want DSS to use. Set the environment variable `DSS_S3_BUCKET_TEST`. If you wish to
run the unit tests, you must create a second S3 bucket to store the test fixtures, and set the environment variable
`DSS_S3_BUCKET_TEST_FIXTURES` to the name of that bucket.

**GCP**: Follow the instructions in https://cloud.google.com/sdk/downloads to get the `gcloud` command line utility.
Next, go to https://console.cloud.google.com/. Select the correct Google user account on the top right and the correct
GCP project in the drop down in the top center. Go to "IAM & Admin", then "Service accounts", then click "Create service
account" and select "Furnish a new private key". Create the account and download the service account key JSON file. Set
the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to the path of the service account key JSON file. Run `gcloud
auth activate-service-account --key-file=/path/to/service-account.json`. Run `gcloud config set project 'PROJECT
NAME'`. Create a bucket on Google Cloud Platform and set the environment variable `DSS_GS_BUCKET_TEST`.  If you wish to
run the unit tests, you must create a second Google Cloud Platform bucket to store the test fixtures, and set the
environment variable `DSS_GS_BUCKET_TEST_FIXTURES` to the name of that bucket.

**Azure**: Set the environment variables `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`.

#### Running the prototype
Run `./dss-api` in this directory.

#### Populate test data

In order to run the tests below you need to have some test data staged into your buckets:

    python tests/fixtures/populate.py --s3-bucket $DSS_S3_TEST_SRC_DATA_BUCKET --gs-bucket $DSS_GS_TEST_SRC_DATA_BUCKET

#### Running tests

Some tests require the Elasticsearch service to be running on the local system:

Run: `elasticsearch`

Then to perform the data store tests:

Run `make test` in this directory.

#### Deployment

Assuming the tests have passed above, the next step is to manually deploy.  See the section below for information on CI/CD with Travis if continuous deployment is your goal.

You will need to ensure you are ready for deployment refer to `.travis.yml` to ensure you have all needed requirements.  Right now the deployment is designed for an Ubuntu Trusty (14.04) with the following packages apt-get installed:

    sudo apt-get install jq moreutils gettext

Currently you need to setup an Elasticsearch hosted instance on AWS for the indexer:

    # TODO

Now deploy using make:

    make deploy

Setup API gateway.  The gateway is automatically setup for you and associated with the Lambda.  However, to get a friendly domain name you need to follow the directions at this [page](http://docs.aws.amazon.com/apigateway/latest/developerguide/how-to-custom-domains.html). In summary:

* generate a HTTPS certificate via AWS Certificate Manager, make sure it's in us-east-1
* setup the domain name in the API gateway console
* setup in Amazon Route 53 to point the domain to the API gateway
* in the API gateway fill in the endpoints for the custom domain name e.g. Path=`/`, Destination=`dss` and `dev`.  These might be different based on the profile used (dev, stage, etc).

If successful you should be able to see the API Swagger docs at:

    https://<domain_name>/v1/

And you should be able to list bundles like this:

    curl -X GET "https://<domain_name>/v1/bundles" -H  "accept: application/json"

Note, the Swagger docs have a hard-coded path for the API endpoint in them.

#### Using the Client

Now that you have deployed the data store, the next step will be to use the CLI to upload and download data to the system.

The client requires you change `hca/api_spec.json` to point to the correct host, schemes, and, possibly, basePath. Note, the port should not be included if using https.

    # list bundles
    hca get-bundles
    # upload full bundle
    hca upload --replica aws --staging-bucket hca-demo-files paired_ends
    # upload a new bundle
    hca upload --replica aws --staging-bucket hca-demo-files paired_ends


Make sure the temp location you're staging to on S3 is accessible to the Lambda IAM role.  Look at the IAM role policy.

#### Checking Indexing

Now that you've uploaded data, the next step is to confirm the indexing is working properly and you can query on the hosted elasticsearch.

Make sure the IAM policy is right, there's a bug where the region is not setup properly:

```
    "Resource":
    "arn:aws:es::719818754276:domain/dss-index-de/*"
    # Becomes
    arn:aws:es:us-west-2:719818754276:domain/dss-index-dev
```

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
[![codecov](https://codecov.io/gh/HumanCellAtlas/data-store/branch/master/graph/badge.svg)](https://codecov.io/gh/HumanCellAtlas/data-store)

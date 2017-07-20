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

#### Environment Variables

Environment variables are required for test and deployment.
The required environment variables and their default values are in the file `environment`
To customize the values of these environment variables:

1. Copy `environment.local.example` to `environment.local`
2. Edit `environment.local` to add custom entries that override the default values in `environment`
    
Run `source environment`  now and whenever these environment files are modified.

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

#### Running the DSS API locally
Run `./dss-api` in the top-level `data-store` directory.

#### Check and install software required to test and deploy
Check that software packages required to test and deploy are available, and install them if necessary.

Run: `make --dry-run`

#### Populate test data

To run the tests, test fixture data must be setup using the following command.
**This command will completely empty the given buckets** before populating them with test fixture data, please 
ensure the correct bucket names are provided.

    tests/fixtures/populate.py --s3-bucket $DSS_S3_BUCKET_TEST_FIXTURES --gs-bucket $DSS_GS_BUCKET_TEST_FIXTURES


#### Running tests

Some tests require the Elasticsearch service to be running on the local system:

Run: `elasticsearch`

Then to perform the data store tests:

Run `make test` in the top-level `data-store` directory.

#### Deployment

Assuming the tests have passed above, the next step is to manually deploy.  See the section below for information on CI/CD with Travis if continuous deployment is your goal.

The AWS Elasticsearch Service is used for metadata indexing.
Currently, the AWS Elasticsearch Service must be configured manually.
The AWS Eslasticsearch Service domain name must either:
* have the value "dss-index-$DSS_DEPLOYMENT_STAGE"
* or, the environment variable `DSS_ES_DOMAIN` must be set to the domain name of the AWS Elasticsearch Service instance to be used.

Note, the Swagger API specification in `dss-api.yml` currently has the `host` set to `hca-dss.czi.technology`.
This may be set to a value appropriate for your deployment (see AWS API Gateway setup below).

Now deploy using make:

    make deploy

Setup AWS API Gateway.  The gateway is automatically setup for you and associated with the Lambda.
However, to get a friendly domain name you need to follow the directions at this [page](http://docs.aws.amazon.com/apigateway/latest/developerguide/how-to-custom-domains.html). In summary:

* generate a HTTPS certificate via AWS Certificate Manager, make sure it's in us-east-1
* setup the domain name in the API gateway console
* setup in Amazon Route 53 to point the domain to the API gateway
* in the API gateway fill in the endpoints for the custom domain name e.g. Path=`/`, Destination=`dss` and `dev`.  These might be different based on the profile used (dev, stage, etc).

If successful, you should be able to see the Swagger API documentation at:

    https://<domain_name>

And you should be able to list bundles like this:

    curl -X GET "https://<domain_name>/v1/bundles" -H  "accept: application/json"


#### Using the HCA Data Store CLI Client

Now that you have deployed the data store, the next step is to use the HCA Data Store CLI to upload and download data to the system.
See [data-store-cli](https://github.com/HumanCellAtlas/data-store-cli) for installation instructions.
The client requires you change `hca/api_spec.json` to point to the correct host, schemes, and, possibly, basePath.
Examples CLI use:

    # list bundles
    hca get-bundles
    # upload full bundle
    hca upload --replica aws --staging-bucket staging_bucket_name data-bundle-examples/smartseq2/paired_ends

#### Checking Indexing

Now that you've uploaded data, the next step is to confirm the indexing is working properly and you can query the indexed metadata.

    hca post-search --query '
    {
        "query": {
            "bool": {
                "must": [{
                    "match": {
                        "files.sample_json.donor.species": "Homo sapiens"
                    }
                }, {
                    "match": {
                        "files.assay_json.single_cell.method": "Fluidigm C1"
                    }
                }, {
                    "match": {
                        "files.sample_json.ncbi_biosample": "SAMN04303778"
                    }
                }]
            }
        }
    }'

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

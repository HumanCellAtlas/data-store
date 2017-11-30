# HCA DSS: The Human Cell Atlas Data Storage System

This repository contains design specs and prototypes for the replicated data storage system (aka the "blue box") of
the [Human Cell Atlas](https://www.humancellatlas.org/). We
use [this Google Drive folder](https://drive.google.com/open?id=0B-_4IWxXwazQbWE5YmtqUWx3RVE) for design docs and
meeting notes, and [this Waffle board](https://waffle.io/HumanCellAtlas/data-store) to track our GitHub work.

#### About this prototype

The prototype in this repository uses [Swagger](http://swagger.io/) to specify the API in [dss-api.yml](dss-api.yml),
and [Connexion](https://github.com/zalando/connexion) to map the API specification to its implementation in Python.

You can use the
[Swagger Editor](http://editor.swagger.io/#/?import=https://raw.githubusercontent.com/HumanCellAtlas/data-store/master/dss-api.yml) to
review and edit the prototype API specification. When the prototype app is running, the Swagger spec is also available
at `/v1/swagger.json`.

The prototype is deployed continuously from the `master` branch, with the resulting producer and consumer API available
at https://dss.staging.data.humancellatlas.org/.

#### Installing dependencies for development on the prototype

The HCA DSS prototype development environment requires Python 3.6+ to run. Run `pip install -r requirements-dev.txt` in
this directory.

#### Installing dependencies for the prototype

The HCA DSS prototype requires Python 3.6+ to run. Run `pip install -r requirements.txt` in this directory.

#### Pull sample data bundles

Tests also use data from the data-bundle-examples subrepository. Run: `git submodule update --init`

#### Environment Variables

Environment variables are required for test and deployment. The required environment variables and their default values
are in the file `environment`. To customize the values of these environment variables:

1. Copy `environment.local.example` to `environment.local`
2. Edit `environment.local` to add custom entries that override the default values in `environment`
    
Run `source environment`  now and whenever these environment files are modified.

#### Configuring cloud-specific access credentials

##### AWS

1. Follow the instructions in http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html to get the
   `aws` command line utility.

2. Create an S3 bucket that you want DSS to use and in `environment.local`, set the environment variable `DSS_S3_BUCKET`
   to the name of that bucket. Make sure the bucket region is consistent with `AWS_DEFAULT_REGION` in
   `environment.local`.

3. If you wish to run the unit tests, you must create two more S3 buckets, one for test data and another for test
   fixtures, and set the environment variables `DSS_S3_BUCKET_TEST` and `DSS_S3_BUCKET_TEST_FIXTURES` to the names of
   those buckets.

Hint: To create S3 buckets from the command line, use `aws s3 mb --region REGION s3://BUCKET_NAME/`. 

##### GCP

1.  Follow the instructions in https://cloud.google.com/sdk/downloads to get the `gcloud` command line utility.

2.  In the [Google Cloud Console](https://console.cloud.google.com/), select the correct Google user account on the top
    right and the correct GCP project in the drop down in the top center. Go to "IAM & Admin", then "Service accounts",
    then click "Create service account" and select "Furnish a new private key". Under "Roles" select "Project – Owner",
    "Project – Service Account Actor" and "Cloud Functions – Cloud Function Developer". Create the account and download
    the service account key JSON file.

3.  In `environment.local`, set the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to the path of the service
    account key JSON file.

4.  Choose a region that has support for Cloud Functions and set `GCP_DEFAULT_REGION` to that region. See
    https://cloud.google.com/about/locations/ for a list of supported regions.

5.  Run `gcloud auth activate-service-account --key-file=/path/to/service-account.json`.

6.  Run `gcloud config set project PROJECT_ID` where PROJECT_ID is the ID, not the name (!) of the GCP project you
    selected earlier.

7.  Enable required APIs: `gcloud service-management enable cloudfunctions.googleapis.com`; `gcloud service-management
    enable runtimeconfig.googleapis.com`

8.  Generate OAuth application secrets to be used for your instance: 

    1) Go to https://console.developers.google.com/apis/credentials (you may have to select Organization and Project
    again)
    
	2) Click *Create Credentials* and select *OAuth client*
    
	3) For *Application type* choose *Other*  
    
	4) Under application name, use `hca-dss-` followed by the stage name i.e., the value of DSS_DEPLOYMENT_STAGE. This
    is a convention only and carries no technical significance.
    
	5) Click *Create*, don't worry about noting the client ID and secret, click *OK*
    
	6) Click the edit icon for the new credentials and click *Download JSON*
    
	7) Place the downloaded JSON file into the project root as `application_secrets.json`

9.  Create a Google Cloud Storage bucket and in `environment.local`, set the environment variable `DSS_GS_BUCKET` to the
    name of that bucket. Make sure the bucket region is consistent with `GCP_DEFAULT_REGION` in `environment.local`.

10. If you wish to run the unit tests, you must create two more buckets, one for test data and another for test
    fixtures, and set the environment variables `DSS_GS_BUCKET_TEST` and `DSS_GS_BUCKET_TEST_FIXTURES` to the names of
    those buckets.

Hint: To create GCS buckets from the command line, use `gsutil mb -c regional -l REGION gs://BUCKET_NAME/`.

##### Azure

1. Set the environment variables `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`.

#### Running the DSS API locally

Run `./dss-api` in the top-level `data-store` directory.

#### Check and install software required to test and deploy

Check that software packages required to test and deploy are available, and install them if necessary.

Run: `make --dry-run`

#### Populate test data

To run the tests, test fixture data must be set up using the following command. **This command will completely empty the
given buckets** before populating them with test fixture data, please ensure the correct bucket names are provided.

    tests/fixtures/populate.py --s3-bucket $DSS_S3_BUCKET_TEST_FIXTURES --gs-bucket $DSS_GS_BUCKET_TEST_FIXTURES


#### Running tests

Set the environment variable `DSS_TEST_ES_PATH` to the path of the `elasticsearch` binary on your machine. Then to
perform the data store tests:

Run `make test` in the top-level `data-store` directory.

#### Deployment

Assuming the tests have passed above, the next step is to manually deploy. See the section below for information on
CI/CD with Travis if continuous deployment is your goal.

The AWS Elasticsearch Service is used for metadata indexing. Currently, the AWS Elasticsearch Service must be configured
manually. The AWS Elasticsearch Service domain name must either:

* have the value `dss-index-$DSS_DEPLOYMENT_STAGE`

* or, the environment variable `DSS_ES_DOMAIN` must be set to the domain name of the AWS Elasticsearch Service instance
  to be used.

For typical development deployments the t2.small.elasticsearch instance type is more than sufficient. 

Now deploy using make:

    make deploy

Set up AWS API Gateway. The gateway is automatically set up for you and associated with the Lambda. However, to get a
friendly domain name, you need to follow the
directions [here](http://docs.aws.amazon.com/apigateway/latest/developerguide/how-to-custom-domains.html). In summary:

1.  Generate a HTTPS certificate via AWS Certificate Manager (ACM). See note below on choosing a region for the
    certificate.

2.  Set up the custom domain name in the API gateway console. See note below on the DNS record type.

3.  In Amazon Route 53 point the domain to the API gateway

4.  In the API Gateway, fill in the endpoints for the custom domain name e.g. Path=`/`, Destination=`dss` and
    `dev`. These might be different based on the profile used (dev, stage, etc).

5.  Set the environment variable `API_HOST` to your domain name in the `environment.local` file.

Note: The certificate should be in the same region as the API gateway or, if that's not possible, in `us-east-1`. If the
ACM certificate's region is `us-east-1` and the API gateway is in another region, the type of the custom domain name
must be *Edge Optimized*. Provisioning such a domain name typically takes up to 40 minutes because the certificate needs
to be replicated to all involved CloudFront edge servers. The corresponding record set in Route 53 needs to be an
**alias** A record, not a CNAME or a regular A record, and it must point to the CloudFront host name associated with the
edge-optimized domain name. Starting November 2017, API gateway supports regional certificates i.e., certificates in
regions other than `us-east-1`. This makes it possible to match the certificate's region with that of the API
gateway. and cuts the provisioning of the custom domain name down to seconds. Simply create the certificate in the same
region as that of the API gateway, create a custom domain name of type *Regional* and in Route53 add a CNAME recordset
that points to the gateway's canonical host name.
 
If successful, you should be able to see the Swagger API documentation at:

    https://<domain_name>

And you should be able to list bundles like this:

    curl -X GET "https://<domain_name>/v1/bundles" -H  "accept: application/json"


#### Using the HCA Data Store CLI Client

Now that you have deployed the data store, the next step is to use the HCA Data Store CLI to upload and download data to
the system. See [data-store-cli](https://github.com/HumanCellAtlas/data-store-cli) for installation instructions. The
client requires you change `hca/api_spec.json` to point to the correct host, schemes, and, possibly, basePath. Examples
of CLI use:

    # list bundles
    hca get-bundles
    # upload full bundle
    hca upload --replica aws --staging-bucket staging_bucket_name data-bundle-examples/smartseq2/paired_ends

#### Checking Indexing

Now that you've uploaded data, the next step is to confirm the indexing is working properly and you can query the
indexed metadata.

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
`scripts/authorize_aws_deploy.sh IAM-PRINCIPAL-TYPE IAM-PRINCIPAL-NAME` (e.g. `authorize_aws_deploy.sh group
travis-ci`) to give that principal the permissions needed to deploy the app. Because a group policy has a higher size
limit (5,120 characters) than a user policy (2,048 characters), it is advisable to apply this to a group and add the
principal to that group. Because this is a limited set of permissions, it does not have write access to IAM. To set up
the IAM policies for resources in your account that the app will use, run `make deploy` using privileged account
credentials once from your workstation. After this is done, Travis CI will be able to deploy on its own. You must
repeat the `make deploy` step from a privileged account any time you change the IAM policies templates in
`iam/policy-templates/`.

[![](https://img.shields.io/badge/slack-%23data--store-557EBF.svg)](https://humancellatlas.slack.com/messages/data-store/)
[![Build Status](https://travis-ci.org/HumanCellAtlas/data-store.svg?branch=master)](https://travis-ci.org/HumanCellAtlas/data-store)
[![codecov](https://codecov.io/gh/HumanCellAtlas/data-store/branch/master/graph/badge.svg)](https://codecov.io/gh/HumanCellAtlas/data-store)
[![Waffle.io - Issues in progress](https://badge.waffle.io/HumanCellAtlas/data-store.svg?label=in%20progress&title=In%20Progress)](http://waffle.io/HumanCellAtlas/data-store)

#### Managing dependencies

The direct runtime dependencies of this project are defined in `requirements.txt.in`. Direct development dependencies
are defined in `requirements-dev.txt.in`. All dependencies, direct and transitive, are defined in the corresponding
`requirements.txt` and `requirements-dev.txt` files. The latter two can be generated using `make requirements.txt` or
`make requirements-dev.txt` respectively. Modifications to any of these four files need to be committed. This process is
aimed at making dependency handling more deterministic without accumulating the upgrade debt that would be incurred by
simply pinning all direct and transitive dependencies.  Avoid being overly restrictive when constraining the allowed
version range of direct dependencies in -`requirements.txt.in` and `requirements-dev.txt.in`

If you need to modify or add a direct runtime dependency declaration, follow the steps below:

1) Make sure there are no pending changes to `requirements.txt` or `requirements-dev.txt`.
2) Make the desired change to `requirements.txt.in` or `requirements-dev.txt.in`
3) Run `make requirements.txt`.  Run `make requirements-dev.txt` if you have modified `requirements-dev.txt.in`.
4) Visually check the changes to `requirements.txt` and `requirements-dev.txt`.
5) Commit them with a message like `Bumping dependencies`.

You now have two commits, one that catches up with updates to transitive dependencies, and one that tracks your explict
change to a direct dependency. This process applies to development dependencies as well, except for
`requirements-dev.txt` and `requirements-dev.txt.in` respectively.

If you wish to re-pin all the dependencies, run `make refresh_all_requirements`.  It is advisable to do a full
test-deploy-test cycle after this (the test after the deploy is required to test the lambdas).
